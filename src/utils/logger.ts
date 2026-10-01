import { AsyncLocalStorage } from "node:async_hooks";
import winston, { Logger as WinstonLogger } from "winston";

const { combine, timestamp, printf, colorize, errors, json, splat } =
  winston.format;

export type LogLevel = "error" | "warn" | "info" | "http" | "debug";

const LOG_LEVEL: LogLevel = (process.env.LOG_LEVEL as LogLevel) || "info";
const IS_PRODUCTION = process.env.NODE_ENV === "production";
const SERVICE_NAME = process.env.SERVICE_NAME || "astro-chat-pacer";

interface TraceContext {
  traceId: string;
}

const traceStorage = new AsyncLocalStorage<TraceContext>();

const consoleFormat = combine(
  colorize({ all: true }),
  timestamp({ format: "YYYY-MM-DD HH:mm:ss.SSS" }),
  errors({ stack: true }),
  splat(),
  printf(({ timestamp, level, message, prefix, traceId, stack, ...meta }) => {
    const scope = prefix ? ` [${prefix}]` : "";
    const trace = traceId ? ` (trace=${traceId})` : "";
    const extras = Object.keys(meta).length ? ` ${JSON.stringify(meta)}` : "";
    return `${timestamp} [${level}]${scope}${trace} ${stack || message}${extras}`;
  }),
);

const jsonFormat = combine(
  timestamp(),
  errors({ stack: true }),
  splat(),
  json(),
);

export class Logger {
  private static root: WinstonLogger = winston.createLogger({
    level: LOG_LEVEL,
    defaultMeta: { service: SERVICE_NAME },
    format: IS_PRODUCTION ? jsonFormat : consoleFormat,
    transports: [new winston.transports.Console()],
    exitOnError: false,
  });

  private readonly prefix?: string;

  constructor(prefix?: string) {
    this.prefix = prefix;
  }

  static runWithTraceId<T>(traceId: string, fn: () => T): T {
    return traceStorage.run({ traceId }, fn);
  }

  /** Set the root log level at runtime (e.g. from DynamicConfig at boot). */
  static setLevel(level: LogLevel): void {
    Logger.root.level = level;
  }

  static get currentTraceId(): string | undefined {
    return traceStorage.getStore()?.traceId;
  }

  child(prefix: string): Logger {
    return new Logger(this.prefix ? `${this.prefix}:${prefix}` : prefix);
  }

  error(
    message: string,
    error?: unknown,
    meta: Record<string, unknown> = {},
  ): void {
    const errorMeta =
      error instanceof Error
        ? { errorMessage: error.message, stack: error.stack }
        : error !== undefined
          ? { error }
          : {};
    this.log("error", message, { ...errorMeta, ...meta });
  }

  warn(message: string, meta: Record<string, unknown> = {}): void {
    this.log("warn", message, meta);
  }

  info(message: string, meta: Record<string, unknown> = {}): void {
    this.log("info", message, meta);
  }

  http(message: string, meta: Record<string, unknown> = {}): void {
    this.log("http", message, meta);
  }

  debug(message: string, meta: Record<string, unknown> = {}): void {
    this.log("debug", message, meta);
  }

  private log(
    level: LogLevel,
    message: string,
    meta: Record<string, unknown>,
  ): void {
    Logger.root.log(level, message, {
      prefix: this.prefix,
      traceId: Logger.currentTraceId,
      ...meta,
    });
  }
}

export const logger = new Logger();
