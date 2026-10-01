import { DynamicConfig } from "./config";
import { HttpApi } from "./http/api";
import { KafkaService } from "./kafka/kafkaService";
import { PaceRunner } from "./pipeline/paceRunner";
import {
  ApiPacingProvider,
  PacingProvider,
  StaticPacingProvider,
} from "./pacing/provider";
import { Logger, logger } from "./utils/logger";

/**
 * astro-chat-pacer — turns a whole brains reply into a paced stream of blocks.
 *
 * One codebase, two deployables, selected by `ROLE`:
 *
 *   consumer  reads `astro_chat_out`, paces each turn, publishes every block to
 *             `astro_chat_paced`. This is the production path. (Dockerfile.consumer)
 *   service   no consumer group — serves the HTTP API and publishes the turns it
 *             is handed. Replay and inspection. (Dockerfile.service)
 *
 * Both roles produce; only `consumer` consumes. Both serve `/health`.
 */
export type Role = "consumer" | "service";

function resolveRole(): Role {
  const raw = (process.env.ROLE || "consumer").trim().toLowerCase();
  if (raw === "consumer" || raw === "service") return raw;
  throw new Error(`unknown ROLE "${raw}" — expected "consumer" or "service"`);
}

async function bootstrap(): Promise<void> {
  const role = resolveRole();
  const c = DynamicConfig.getInstance().all;
  Logger.setLevel(c.logging.level);

  const envBrokers = (process.env.KAFKA_BROKERS || "")
    .split(",")
    .map((b) => b.trim())
    .filter(Boolean);
  const brokers = envBrokers.length > 0 ? envBrokers : c.kafka.brokers;

  if (!c.kafka.enabled || brokers.length === 0) {
    throw new Error(
      "kafka is disabled or has no brokers — the pacer cannot publish",
    );
  }

  const consuming = role === "consumer";
  const kafka = new KafkaService({
    brokers,
    clientId: process.env.KAFKA_CLIENT_ID || `${c.kafka.clientId}-${role}`,
    // All consumer pods share ONE group: a turn is paced exactly once, and the
    // partitions are split across pods.
    groupId: process.env.KAFKA_GROUP_ID || c.kafka.groupId,
    inputTopic: process.env.KAFKA_INPUT_TOPIC || c.kafka.inputTopic,
    outputTopic: process.env.KAFKA_OUTPUT_TOPIC || c.kafka.outputTopic,
    consume: consuming,
  });

  const { api, ...pacingDefaults } = c.pacing;
  const pacingProvider: PacingProvider = api.enabled
    ? new ApiPacingProvider(api, pacingDefaults)
    : new StaticPacingProvider(pacingDefaults);

  const runner = new PaceRunner(kafka, pacingProvider);

  if (consuming) {
    kafka.onResponse((res) => runner.enqueue(res));
    kafka.onError((err) => {
      // Error envelopes carry no outputs — nothing to pace.
      logger.warn("brains error envelope — not paced", {
        code: err.code,
        message: err.message,
      });
    });
  }

  const http = new HttpApi(
    {
      host: process.env.HOST || c.server.host,
      port: Number(process.env.PORT) || c.server.port,
      role,
      consuming,
    },
    runner,
  );

  await kafka.start();
  await http.start();

  logger.info("astro-chat-pacer started", {
    role,
    env: c.env,
    brokers,
    inputTopic: consuming ? c.kafka.inputTopic : "(not consuming)",
    outputTopic: c.kafka.outputTopic,
    typingSpeedCps: pacingDefaults.typingSpeedCps,
    pacingApi: api.enabled ? api.baseUrl : "disabled (platform defaults)",
  });

  const shutdown = async (signal: string): Promise<void> => {
    logger.info("received shutdown signal", { signal });
    // Stop taking new work first; keep the producer up so in-flight playbacks
    // can finish publishing their remaining blocks, then disconnect.
    await http.stop();
    await kafka.stopConsuming();
    await runner.drain(c.server.shutdownDrainMs);
    await kafka.stop();
    process.exit(0);
  };

  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));

  process.on("unhandledRejection", (reason) => {
    logger.error("unhandled rejection", reason);
  });
  process.on("uncaughtException", (err) => {
    logger.error("uncaught exception", err);
  });
}

if (require.main === module) {
  bootstrap().catch((err) => {
    logger.error("failed to start astro-chat-pacer", err);
    process.exit(1);
  });
}
