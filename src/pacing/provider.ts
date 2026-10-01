import { PacingApiSettings, PacingSettings } from "../config/types";
import { BaseClass } from "../utils/baseClass";

/** Resolves the pacing config that applies to one astrologer's reply. */
export interface PacingProvider {
  get(astrologerId?: number | string | null): Promise<PacingSettings>;
}

/**
 * Platform defaults for every reply, from `config/config.json`.
 *
 * This is the whole implementation today: the per-astrologer pacing API has no
 * spec yet. When it lands, implement {@link ApiPacingProvider} below and select
 * it in `src/index.ts` on `pacing.api.enabled`.
 */
export class StaticPacingProvider implements PacingProvider {
  constructor(private readonly defaults: PacingSettings) {}

  async get(): Promise<PacingSettings> {
    return this.defaults;
  }
}

/**
 * Per-astrologer pacing from the pacing API, cached per astrologer for
 * `cacheTtlMs`, falling back to the platform defaults on any failure — a turn
 * must never fail to be delivered because a pacing lookup did.
 *
 * NOT IMPLEMENTED: waiting on the endpoint + response body spec. The shape the
 * service needs is `typing_speed_cps` and `initial_delay_ms` per astrologer
 * (the `astro_config` columns); the remaining clamps stay platform-wide unless
 * the API serves them too.
 */
export class ApiPacingProvider extends BaseClass implements PacingProvider {
  constructor(
    private readonly api: PacingApiSettings,
    private readonly defaults: PacingSettings,
  ) {
    super("PacingProvider");
  }

  async get(astrologerId?: number | string | null): Promise<PacingSettings> {
    this.warn("pacing API not implemented — using platform defaults", {
      astrologerId,
      baseUrl: this.api.baseUrl,
    });
    return this.defaults;
  }
}
