export type LogLevel = "error" | "warn" | "info" | "http" | "debug";

/** Kafka transport: one input topic consumed, one output topic produced. */
export interface KafkaSettings {
  enabled: boolean;
  /** Broker `host:port` list (usually injected via `KAFKA_BROKERS`). */
  brokers: string[];
  clientId: string;
  /** Consumer group for the input topic; a per-pod suffix is appended at boot. */
  groupId: string;
  /** Topic consumed — brains replies (`astro_chat_out`). */
  inputTopic: string;
  /** Topic produced — one message per paced block (`astro_chat_paced`). */
  outputTopic: string;
}

/** Plain HTTP health/readiness listener. */
export interface ServerSettings {
  host: string;
  port: number;
  /** Grace period for in-flight playbacks on shutdown, ms. */
  shutdownDrainMs: number;
}

/**
 * Pacing knobs. `typingSpeedCps` is *perceived* characters per second — the dial
 * that makes a pause proportionate to a block's length, not a keystroke rate.
 *
 * These are the platform fallback. Per-astrologer values come from the pacing
 * API (`pacing.api`) once that endpoint exists; see `src/pacing/provider.ts`.
 */
export interface PacingSettings {
  typingSpeedCps: number;
  /** Opening beat added to the FIRST block only, ms. */
  initialDelayMs: number;
  /** Floor on any single pause, ms. */
  minDelayMs: number;
  /** Ceiling on any single pause, ms. */
  maxDelayMs: number;
  /** Ceiling on the whole reply's pauses, ms; exceeded plans are scaled down. */
  totalMaxMs: number;
  /** Pause before the pills block when it rides alone (no text), ms. */
  pillsDelayMs: number;
}

/**
 * Per-astrologer pacing lookup. Disabled until the API spec lands — every reply
 * then paces on {@link PacingSettings}.
 */
export interface PacingApiSettings {
  enabled: boolean;
  /** Base URL of the service that serves an astrologer's pacing config. */
  baseUrl: string;
  /** Per-request timeout, ms. */
  timeoutMs: number;
  /** How long a fetched pacing config is reused before refetching, ms. */
  cacheTtlMs: number;
}

export interface LoggingConfig {
  level: LogLevel;
  pretty: boolean;
}

export interface AppConfig {
  env: string;
  server: ServerSettings;
  kafka: KafkaSettings;
  pacing: PacingSettings & { api: PacingApiSettings };
  logging: LoggingConfig;
}

/** Recursive partial used for environment override files. */
export type DeepPartial<T> = {
  [K in keyof T]?: T[K] extends Array<infer U>
    ? Array<U>
    : T[K] extends object
      ? DeepPartial<T[K]>
      : T[K];
};
