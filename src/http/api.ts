import { createServer, IncomingMessage, Server as HttpServer } from "node:http";
import { ResponseEnvelope } from "../kafka/types";
import { PaceRunner } from "../pipeline/paceRunner";
import { BaseClass } from "../utils/baseClass";

/** Largest body the API will read, bytes. A turn is a few KB. */
const MAX_BODY_BYTES = 256 * 1024;

export interface ApiOptions {
  host: string;
  port: number;
  /** Role name reported by `/health`. */
  role: string;
  /** True when this process also consumes `astro_chat_out`. */
  consuming: boolean;
}

/**
 * The HTTP face of the pacer — health, plus manual publish/replay.
 *
 * `POST /turns/pace` takes an `astro_chat_out` turn verbatim and paces it onto
 * `astro_chat_paced` exactly as the consumer would: the same plan, the same
 * pauses, the same messages. It is how a turn is replayed without putting it
 * back on the input topic, and how pacing is eyeballed before it reaches a user.
 *
 * Routes:
 *   GET  /health       — liveness + how many turns are mid-playback
 *   POST /turns/pace   — body: a ResponseEnvelope (+ optional `dry_run: true`)
 */
export class HttpApi extends BaseClass {
  private readonly server: HttpServer;

  constructor(
    private readonly options: ApiOptions,
    private readonly runner: PaceRunner,
  ) {
    super("HttpApi");
    this.server = createServer((req, res) => {
      void this.route(req)
        .then(({ status, body }) => {
          const payload = JSON.stringify(body);
          res.writeHead(status, { "content-type": "application/json" });
          res.end(payload);
        })
        .catch((err) => {
          this.error("request failed", err, { url: req.url });
          res.writeHead(500, { "content-type": "application/json" });
          res.end(JSON.stringify({ error: "internal_error" }));
        });
    });
  }

  async start(): Promise<void> {
    await new Promise<void>((resolve) => {
      this.server.listen(this.options.port, this.options.host, resolve);
    });
    this.log("http api listening", {
      host: this.options.host,
      port: this.options.port,
      role: this.options.role,
    });
  }

  async stop(): Promise<void> {
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
  }

  private async route(
    req: IncomingMessage,
  ): Promise<{ status: number; body: unknown }> {
    const path = req.url?.split("?")[0] ?? "/";

    if (req.method === "GET" && path === "/health") {
      return {
        status: 200,
        body: {
          status: "ok",
          role: this.options.role,
          consuming: this.options.consuming,
          pending: this.runner.pending,
        },
      };
    }

    if (req.method === "POST" && path === "/turns/pace") {
      return this.paceTurn(req);
    }

    return { status: 404, body: { error: "not_found" } };
  }

  /**
   * Pace a turn handed in over HTTP. Returns the plan immediately (202) and
   * publishes the blocks on their beats in the background — the caller is not
   * held for the length of the reply. `dry_run` returns the same plan (200)
   * without publishing anything.
   */
  private async paceTurn(
    req: IncomingMessage,
  ): Promise<{ status: number; body: unknown }> {
    let parsed: unknown;
    try {
      parsed = JSON.parse(await readBody(req));
    } catch (err) {
      return {
        status: 400,
        body: { error: "invalid_json", message: (err as Error).message },
      };
    }

    const invalid = validateEnvelope(parsed);
    if (invalid) return { status: 400, body: { error: invalid } };

    const body = parsed as ResponseEnvelope & { dry_run?: boolean };
    const dryRun = body.dry_run === true;
    const plan = await this.runner.plan(body);

    if (plan.blocks.length === 0) {
      return { status: 422, body: { error: "no_publishable_block" } };
    }

    if (!dryRun) this.runner.enqueue(body);

    this.log(dryRun ? "planned turn (dry run)" : "accepted turn for pacing", {
      sessionRef: body.external_session_ref,
      queryRef: body.external_query_ref,
      blockCount: plan.blocks.length,
      totalMs: plan.totalMs,
    });

    return {
      status: dryRun ? 200 : 202,
      body: {
        accepted: !dryRun,
        external_session_ref: body.external_session_ref,
        external_query_ref: body.external_query_ref,
        block_count: plan.blocks.length,
        total_ms: plan.totalMs,
        typing_speed_cps: plan.typingSpeedCps,
        blocks: plan.blocks,
      },
    };
  }
}

/** Read a request body, refusing anything over {@link MAX_BODY_BYTES}. */
function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let size = 0;
    req.on("data", (chunk: Buffer) => {
      size += chunk.length;
      if (size > MAX_BODY_BYTES) {
        reject(new Error("body too large"));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    req.on("error", reject);
  });
}

/** Returns an error code when the body isn't a usable turn, else undefined. */
function validateEnvelope(value: unknown): string | undefined {
  if (!value || typeof value !== "object") return "body_must_be_an_object";
  const turn = value as Partial<ResponseEnvelope>;
  if (
    typeof turn.external_session_ref !== "string" ||
    !turn.external_session_ref
  ) {
    return "external_session_ref_required";
  }
  if (!Array.isArray(turn.outputs)) return "outputs_required";
  return undefined;
}
