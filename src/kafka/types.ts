// Wire types for the two topics this service bridges.
//
//   astro_chat_out  --> ResponseEnvelope (one per turn, all blocks at once)
//   astro_chat_paced --> PacedBlockMessage (one per block, published on its beat)

export interface TextOutput {
  type: "text";
  /** GitHub-flavored Markdown. */
  content: string;
  subject?: string | null;
}

/** A quick-action suggestion attached to a reply. Passed through verbatim. */
export interface PillItem {
  kind: string;
  label: string;
  send_text?: string | null;
  deeplink_id?: number | null;
  deeplink_type?: string | null;
  url?: string | null;
}

export interface PillsOutput {
  type: "pills";
  items: PillItem[];
}

/**
 * One output block. `text` and `pills` are known today; unknown/future block
 * types are tolerated — they are forwarded unchanged, paced like a text block
 * of zero length.
 */
export type OutputBlock =
  TextOutput | PillsOutput | { type: string; [key: string]: unknown };

export type Decision =
  "continue" | "terminate_resolved" | "escalate" | "terminate_unresolved";

/** Success value on `astro_chat_out` — the whole turn in one message. */
export interface ResponseEnvelope {
  external_query_ref: string;
  external_session_ref: string;
  speaker_agent_id: number | null;
  /**
   * The astrologer whose pacing config applies. Not on the wire yet — brains
   * adds it shortly; until then pacing falls back to the config defaults.
   */
  astrologer_id?: number | string | null;
  /** Ordered; always ≥ 1 `text` block. */
  outputs: OutputBlock[];
  decision: Decision;
}

export type ErrorCode =
  | "agent_not_found"
  | "owner_mismatch"
  | "duplicate_in_flight"
  | "authoring_invalid"
  | "brains_error";

/** Error value on `astro_chat_out`, produced in place of a ResponseEnvelope. */
export interface ErrorEnvelope {
  code: ErrorCode;
  message: string;
}

/** An ErrorEnvelope is distinguished from a ResponseEnvelope by its `code`. */
export function isErrorEnvelope(
  value: ResponseEnvelope | ErrorEnvelope,
): value is ErrorEnvelope {
  return typeof (value as ErrorEnvelope).code === "string";
}

/**
 * One message on `astro_chat_paced`: a single block of a reply, produced at the
 * moment it should appear. The service has already held the pause, so a consumer
 * renders it on arrival — `typing_ms` is reported for tracing, not to be waited
 * on a second time.
 *
 * Keyed by `external_session_ref`, so a conversation's blocks stay ordered on
 * one partition.
 */
export interface PacedBlockMessage {
  external_query_ref: string;
  external_session_ref: string;
  speaker_agent_id: number | null;
  astrologer_id?: number | string | null;
  /** 0-based position of this block among the blocks actually published. */
  block_index: number;
  /** How many blocks this turn will publish in total. */
  block_count: number;
  /** True on the final block of the turn. */
  is_last: boolean;
  /** The block itself, forwarded verbatim from `astro_chat_out`. */
  output: OutputBlock;
  /**
   * Quick actions for the turn, attached to the LAST text block (brains sends
   * them as their own `pills` block; this service folds them in so they land
   * with the closing bubble). Absent when the turn had none.
   */
  pills?: PillItem[];
  /** The pause this service held before publishing, ms. Tracing only. */
  typing_ms: number;
  /** Turn decision, copied onto every block of the turn. */
  decision: Decision;
  /** Epoch ms at publish time. */
  published_at: number;
}
