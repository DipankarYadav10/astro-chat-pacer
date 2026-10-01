import { Consumer, Kafka, Producer, logLevel } from "kafkajs";
import { BaseClass } from "../utils/baseClass";
import {
  ErrorEnvelope,
  PacedBlockMessage,
  ResponseEnvelope,
  isErrorEnvelope,
} from "./types";

export interface KafkaConfig {
  brokers: string[];
  clientId: string;
  groupId: string;
  /** Consumed — brains replies (`astro_chat_out`). */
  inputTopic: string;
  /** Produced — one message per paced block (`astro_chat_paced`). */
  outputTopic: string;
  /**
   * Subscribe to {@link inputTopic}. False in the API role, which only ever
   * produces — it must not join the consumer group and steal partitions.
   */
  consume: boolean;
}

export type ResponseHandler = (res: ResponseEnvelope) => Promise<void> | void;
export type ErrorHandler = (err: ErrorEnvelope) => Promise<void> | void;

/** Bounded set of external_query_refs already handled (at-least-once dedupe). */
const MAX_SEEN = 10_000;

/**
 * Kafka transport for the pacer:
 *
 *   inputTopic  --> ResponseEnvelope | ErrorEnvelope --> handlers
 *   handlers    --> PacedBlockMessage (one per block) --> outputTopic
 *
 * Both topics are keyed by `external_session_ref`, so a conversation stays on
 * one partition and its blocks keep their order. Delivery is at-least-once, so
 * a turn is de-duplicated by `external_query_ref` before it is paced — pacing a
 * redelivered turn would replay the whole reply to the user.
 */
export class KafkaService extends BaseClass {
  private readonly kafka: Kafka;
  private readonly producer: Producer;
  /** Absent in the produce-only (API) role. */
  private readonly consumer?: Consumer;
  private responseHandler?: ResponseHandler;
  private errorHandler?: ErrorHandler;
  private running = false;

  private readonly seen = new Set<string>();
  private readonly seenOrder: string[] = [];

  constructor(private readonly config: KafkaConfig) {
    super("KafkaService");
    this.kafka = new Kafka({
      clientId: config.clientId,
      brokers: config.brokers,
      logLevel: logLevel.WARN,
    });
    this.producer = this.kafka.producer({ allowAutoTopicCreation: true });
    this.consumer = config.consume
      ? this.kafka.consumer({
          groupId: config.groupId,
          allowAutoTopicCreation: true,
        })
      : undefined;
  }

  /** Register the success-response handler. Call before start(). */
  onResponse(handler: ResponseHandler): void {
    this.responseHandler = handler;
  }

  /** Register the error-envelope handler. Call before start(). */
  onError(handler: ErrorHandler): void {
    this.errorHandler = handler;
  }

  /**
   * Connect the producer and, when configured to consume, begin consuming the
   * input topic.
   */
  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;

    await this.producer.connect();

    if (this.consumer) {
      await this.consumer.connect();
      await this.consumer.subscribe({
        topic: this.config.inputTopic,
        fromBeginning: false,
      });
      await this.consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          await this.handleMessage(message.value, {
            topic,
            partition,
            offset: message.offset,
          });
        },
      });
    }

    this.log("kafka started", {
      brokers: this.config.brokers,
      inputTopic: this.consumer ? this.config.inputTopic : "(not consuming)",
      outputTopic: this.config.outputTopic,
      groupId: this.consumer ? this.config.groupId : "(not consuming)",
    });
  }

  /**
   * Produce one paced block, keyed by `external_session_ref`. Throws on failure
   * so the caller can decide whether the rest of the turn is still worth
   * publishing — a half-delivered reply is worth knowing about.
   */
  async publishBlock(message: PacedBlockMessage): Promise<void> {
    if (!this.running) {
      throw new Error("kafka not running — cannot publish paced block");
    }
    await this.producer.send({
      topic: this.config.outputTopic,
      messages: [
        {
          key: message.external_session_ref,
          value: JSON.stringify(message),
        },
      ],
    });
    this.debug("published paced block", {
      sessionRef: message.external_session_ref,
      queryRef: message.external_query_ref,
      blockIndex: message.block_index,
      blockCount: message.block_count,
      typingMs: message.typing_ms,
    });
  }

  /** Parse, classify, dedupe and dispatch a single input record. */
  private async handleMessage(
    value: Buffer | null,
    ctx: Record<string, unknown>,
  ): Promise<void> {
    if (!value) return;

    let parsed: ResponseEnvelope | ErrorEnvelope;
    try {
      parsed = JSON.parse(value.toString("utf8"));
    } catch (err) {
      this.error("skipping unparseable message", err, ctx);
      return;
    }

    if (isErrorEnvelope(parsed)) {
      try {
        await this.errorHandler?.(parsed);
      } catch (err) {
        this.error("error handler failed", err, { ...ctx, code: parsed.code });
      }
      return;
    }

    if (!parsed?.external_session_ref || !Array.isArray(parsed.outputs)) {
      this.warn("skipping malformed response envelope", { ...ctx, parsed });
      return;
    }

    if (!this.markSeen(parsed.external_query_ref)) {
      this.debug("skipping duplicate turn", {
        ...ctx,
        queryRef: parsed.external_query_ref,
      });
      return;
    }

    try {
      await this.responseHandler?.(parsed);
    } catch (err) {
      // Swallow so one bad turn doesn't wedge the consumer replaying an offset.
      this.error("response handler failed", err, {
        ...ctx,
        sessionRef: parsed.external_session_ref,
      });
    }
  }

  /** Record a query ref as handled; false if already seen. Bounded LRU-ish. */
  private markSeen(queryRef: string): boolean {
    if (!queryRef) return true; // can't dedupe without a ref — handle it.
    if (this.seen.has(queryRef)) return false;
    this.seen.add(queryRef);
    this.seenOrder.push(queryRef);
    if (this.seenOrder.length > MAX_SEEN) {
      const evicted = this.seenOrder.shift();
      if (evicted) this.seen.delete(evicted);
    }
    return true;
  }

  /**
   * Stop taking new turns off the input topic while leaving the producer up —
   * in-flight playbacks still have blocks to publish. Call before draining.
   */
  async stopConsuming(): Promise<void> {
    if (!this.consumer) return;
    try {
      await this.consumer.disconnect();
      this.log("consumer disconnected — no new turns will be taken");
    } catch (err) {
      this.error("failed to disconnect consumer", err);
    }
  }

  /** Disconnect both clients (best-effort). Drain playbacks first. */
  async stop(): Promise<void> {
    if (!this.running) return;
    this.running = false;
    await this.stopConsuming();
    try {
      await this.producer.disconnect();
    } catch (err) {
      this.error("failed to disconnect producer", err);
    }
    this.log("kafka stopped");
  }
}
