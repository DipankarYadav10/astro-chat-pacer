import { Logger } from "./logger";

export abstract class BaseClass {
  protected readonly logger: Logger;

  constructor(prefix?: string) {
    this.logger = new Logger(prefix ?? this.constructor.name);
  }

  protected log(message: string, meta: Record<string, unknown> = {}): void {
    this.logger.info(message, meta);
  }

  protected debug(message: string, meta: Record<string, unknown> = {}): void {
    this.logger.debug(message, meta);
  }

  protected warn(message: string, meta: Record<string, unknown> = {}): void {
    this.logger.warn(message, meta);
  }

  protected error(
    message: string,
    error?: unknown,
    meta: Record<string, unknown> = {},
  ): void {
    this.logger.error(message, error, meta);
  }
}
