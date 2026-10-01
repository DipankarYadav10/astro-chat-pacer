import { KafkaService } from "../kafka/kafkaService";
import { PacedBlockMessage, ResponseEnvelope } from "../kafka/types";
import { PacingProvider } from "../pacing/provider";
import { PacedStep, planDuration, planTurn } from "../pacing/pacing";
import { PacingSettings } from "../config/types";
import { BaseClass } from "../utils/baseClass";

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

/** A turn's pacing plan, as reported by the API. */
export interface TurnPlan {
  pacing: PacingSettings;
  typingSpeedCps: number;
  /** Wall-clock the whole turn will take, ms. */
  totalMs: number;
  blocks: {
    block_index: number;
    type: string;
    typing_ms: number;
    has_pills: boolean;
  }[];
}

/**
 * The service's one job: take a turn off `astro_chat_out` and republish it to
 * `astro_chat_paced` block by block, holding each block's pause before it goes
 * out — so a downstream consumer can forward each message the moment it arrives
 * and the reply lands like a person typing.
 *
 * Playback is detached from the Kafka handler (a turn spans seconds and must not
 * hold a partition), but serialised **per session**: a session's turns queue
 * behind each other, so two turns can never interleave their blocks. Different
 * sessions pace concurrently.
 */
export class PaceRunner extends BaseClass {
  /** Tail of the in-flight playback chain per session ref. */
  private readonly queues = new Map<string, Promise<void>>();
  private inFlight = 0;

  constructor(
    private readonly kafka: KafkaService,
    private readonly pacingProvider: PacingProvider,
  ) {
    super("PaceRunner");
  }

  /** Turns currently being paced — read by the health endpoint and shutdown. */
  get pending(): number {
    return this.inFlight;
  }

  /**
   * The plan for a turn, without publishing it — what the API returns, and what
   * {@link play} then walks.
   */
  async plan(res: ResponseEnvelope): Promise<TurnPlan> {
    const pacing = await this.pacingProvider.get(res.astrologer_id);
    const steps = planTurn(res.outputs, pacing);
    return {
      pacing,
      typingSpeedCps: pacing.typingSpeedCps,
      totalMs: planDuration(steps),
      blocks: steps.map((step, index) => ({
        block_index: index,
        type: String(step.output.type),
        typing_ms: step.delayMs,
        has_pills: !!step.pills,
      })),
    };
  }

  /**
   * Queue a turn for paced publication and return immediately. Called straight
   * from the Kafka consumer, so it must never await the playback itself.
   */
  enqueue(res: ResponseEnvelope): void {
    const key = res.external_session_ref;
    const previous = this.queues.get(key) ?? Promise.resolve();

    this.inFlight += 1;
    const next = previous
      .then(() => this.play(res))
      .catch((err) => {
        this.error("paced playback failed", err, {
          sessionRef: key,
          queryRef: res.external_query_ref,
        });
      })
      .finally(() => {
        this.inFlight -= 1;
        // Drop the queue entry once this was the last turn on it.
        if (this.queues.get(key) === next) this.queues.delete(key);
      });

    this.queues.set(key, next);
  }

  /** Wait for every queued playback to finish, or until `timeoutMs` elapses. */
  async drain(timeoutMs: number): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (this.inFlight > 0 && Date.now() < deadline) {
      await delay(100);
    }
    if (this.inFlight > 0) {
      this.warn("shutting down with playbacks still in flight", {
        pending: this.inFlight,
      });
    }
  }

  /** Hold each block's pause, then publish it. */
  private async play(res: ResponseEnvelope): Promise<void> {
    const pacing = await this.pacingProvider.get(res.astrologer_id);
    const steps = planTurn(res.outputs, pacing);

    if (steps.length === 0) {
      this.warn("turn had no publishable block — nothing to pace", {
        sessionRef: res.external_session_ref,
        queryRef: res.external_query_ref,
      });
      return;
    }

    this.log("pacing turn", {
      sessionRef: res.external_session_ref,
      queryRef: res.external_query_ref,
      astrologerId: res.astrologer_id ?? null,
      blockCount: steps.length,
      typingSpeedCps: pacing.typingSpeedCps,
      totalMs: planDuration(steps),
    });

    for (const [index, step] of steps.entries()) {
      if (step.delayMs > 0) await delay(step.delayMs);
      try {
        await this.kafka.publishBlock(
          this.toMessage(res, step, index, steps.length),
        );
      } catch (err) {
        // Stop the turn: the blocks after this one would arrive out of context.
        this.error(
          "failed to publish paced block — dropping rest of turn",
          err,
          {
            sessionRef: res.external_session_ref,
            queryRef: res.external_query_ref,
            blockIndex: index,
            blockCount: steps.length,
          },
        );
        return;
      }
    }
  }

  private toMessage(
    res: ResponseEnvelope,
    step: PacedStep,
    index: number,
    total: number,
  ): PacedBlockMessage {
    return {
      external_query_ref: res.external_query_ref,
      external_session_ref: res.external_session_ref,
      speaker_agent_id: res.speaker_agent_id ?? null,
      astrologer_id: res.astrologer_id ?? null,
      block_index: index,
      block_count: total,
      is_last: index === total - 1,
      output: step.output,
      ...(step.pills ? { pills: step.pills } : {}),
      typing_ms: step.delayMs,
      decision: res.decision,
      published_at: Date.now(),
    };
  }
}
