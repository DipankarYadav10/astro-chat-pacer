import * as fs from "fs";
import * as path from "path";
import { AppConfig, DeepPartial } from "./types";

/**
 * DynamicConfig loads configuration by layering an environment-specific override
 * file on top of the base config:
 *
 *   1. `config/config.json`              — base defaults (full config)
 *   2. `config/<NODE_ENV>/config.json`   — environment override (partial, optional)
 *
 * The environment is resolved from `NODE_ENV` (defaulting to "development").
 * The two layers are deep-merged, with the environment layer winning.
 *
 * The class is a singleton: call `DynamicConfig.getInstance()` to access the
 * loaded config anywhere in the app. Call `reload()` to re-read from disk at
 * runtime without restarting the process.
 */
export class DynamicConfig {
  private static instance: DynamicConfig | null = null;

  private readonly configRoot: string;
  private readonly env: string;
  private config: AppConfig;

  private constructor(configRoot: string) {
    this.configRoot = configRoot;
    this.env = DynamicConfig.resolveEnv();
    this.config = this.load();
  }

  /** Resolve the active environment from NODE_ENV. */
  static resolveEnv(): string {
    const env = (process.env.NODE_ENV || "staging").trim().toLowerCase();
    return env.length > 0 ? env : "development";
  }

  /**
   * Get (and lazily create) the singleton instance.
   * @param configRoot Directory holding config.json and the env folders.
   *                   Defaults to `<cwd>/config`.
   */
  static getInstance(configRoot?: string): DynamicConfig {
    if (!DynamicConfig.instance) {
      const root = configRoot ?? path.resolve(process.cwd(), "config");
      DynamicConfig.instance = new DynamicConfig(root);
    }
    return DynamicConfig.instance;
  }

  /** Load base + environment override from disk and deep-merge them. */
  private load(): AppConfig {
    const basePath = path.join(this.configRoot, "config.json");
    const envPath = path.join(this.configRoot, this.env, "config.json");

    const base = DynamicConfig.readJson<AppConfig>(basePath, true);
    const override = DynamicConfig.readJson<DeepPartial<AppConfig>>(
      envPath,
      false,
    );

    const merged = DynamicConfig.deepMerge(base, override ?? {}) as AppConfig;
    merged.env = this.env;
    return merged;
  }

  /** Re-read configuration from disk. Returns the freshly loaded config. */
  reload(): AppConfig {
    this.config = this.load();
    return this.config;
  }

  /** The fully merged configuration object. */
  get all(): AppConfig {
    return this.config;
  }

  /** The active environment name. */
  get environment(): string {
    return this.env;
  }

  /**
   * Read a value by dot-path, e.g. `get("server.https.certPath")`.
   * Falls back to `defaultValue` when the path is absent.
   */
  get<T = unknown>(dotPath: string, defaultValue?: T): T {
    const value = dotPath.split(".").reduce<unknown>((acc, key) => {
      if (acc && typeof acc === "object" && key in (acc as object)) {
        return (acc as Record<string, unknown>)[key];
      }
      return undefined;
    }, this.config);

    return (value === undefined ? defaultValue : value) as T;
  }

  // ---- helpers -------------------------------------------------------------

  private static readJson<T>(filePath: string, required: boolean): T | null {
    if (!fs.existsSync(filePath)) {
      if (required) {
        throw new Error(`Required config file not found: ${filePath}`);
      }
      return null;
    }
    try {
      const raw = fs.readFileSync(filePath, "utf-8");
      return JSON.parse(raw) as T;
    } catch (err) {
      throw new Error(
        `Failed to parse config file ${filePath}: ${(err as Error).message}`,
      );
    }
  }

  /** Deep-merge `source` onto `target`. Arrays and primitives are replaced. */
  private static deepMerge<T>(target: T, source: DeepPartial<T>): T {
    if (
      !DynamicConfig.isPlainObject(target) ||
      !DynamicConfig.isPlainObject(source)
    ) {
      return (source as unknown as T) ?? target;
    }

    const out: Record<string, unknown> = {
      ...(target as Record<string, unknown>),
    };

    for (const [key, srcVal] of Object.entries(
      source as Record<string, unknown>,
    )) {
      if (srcVal === undefined) continue;
      const tgtVal = (target as Record<string, unknown>)[key];

      if (
        DynamicConfig.isPlainObject(tgtVal) &&
        DynamicConfig.isPlainObject(srcVal)
      ) {
        out[key] = DynamicConfig.deepMerge(
          tgtVal,
          srcVal as DeepPartial<unknown>,
        );
      } else {
        out[key] = srcVal;
      }
    }

    return out as T;
  }

  private static isPlainObject(
    value: unknown,
  ): value is Record<string, unknown> {
    return typeof value === "object" && value !== null && !Array.isArray(value);
  }
}
