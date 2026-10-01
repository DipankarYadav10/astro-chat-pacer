import { PacingSettings } from "../config/types";
import { OutputBlock, PillItem, PillsOutput, TextOutput } from "../kafka/types";

/** One block to publish, and the pause to hold before publishing it. */
export interface PacedStep {
  output: OutputBlock;
  /** Quick actions folded onto this block (the last text block only). */
  pills?: PillItem[];
  /** Hold this long *before* publishing, ms. The first step carries the lead. */
  delayMs: number;
}

export function isTextBlock(block: OutputBlock): block is TextOutput {
  return (
    block?.type === "text" && typeof (block as TextOutput).content === "string"
  );
}

export function isPillsBlock(block: OutputBlock): block is PillsOutput {
  return block?.type === "pills" && Array.isArray((block as PillsOutput).items);
}

function clamp(value: number, lo: number, hi: number): number {
  return Math.min(hi, Math.max(lo, value));
}

/**
 * Plan the paced publication of one turn's `outputs`.
 *
 * - Every `text` block becomes its own step, in order. Blank blocks are dropped.
 * - Every `pills` block's items are flattened and folded onto the LAST text
 *   step, so quick actions land with the closing bubble rather than as an empty
 *   one. A turn with pills and no text publishes the pills block on its own.
 * - Unknown block types are forwarded as their own step with the minimum pause.
 *
 * Pause per step: `clamp(min, max, 1000 * length / typingSpeedCps)`, plus
 * `initialDelayMs` on the first step. If the pauses sum past `totalMaxMs` they
 * are scaled down proportionally, each still floored at `minDelayMs`.
 *
 * Deterministic — no jitter — so a redelivered turn would pace identically.
 */
export function planTurn(
  outputs: OutputBlock[],
  pacing: PacingSettings,
): PacedStep[] {
  const pills: PillItem[] = [];
  const steps: PacedStep[] = [];

  for (const block of outputs) {
    if (isPillsBlock(block)) {
      pills.push(...block.items);
      continue;
    }
    if (isTextBlock(block)) {
      const content = block.content.trim();
      if (!content) continue;
      steps.push({
        output: block,
        delayMs: lengthPause(content.length, pacing),
      });
      continue;
    }
    // Unknown type: forward it, but don't pretend to know how long it "types".
    if (block && typeof block.type === "string") {
      steps.push({ output: block, delayMs: pacing.minDelayMs });
    }
  }

  if (steps.length > 0) {
    steps[0].delayMs += Math.max(0, pacing.initialDelayMs);
  }

  scaleToCeiling(steps, pacing);

  if (pills.length > 0) {
    const lastText = lastTextStep(steps);
    if (lastText) {
      lastText.pills = pills;
    } else {
      steps.push({
        output: { type: "pills", items: pills },
        delayMs: Math.max(0, pacing.pillsDelayMs),
      });
    }
  }

  return steps;
}

/** Total wall-clock a plan will take, ms — for logs and tracing. */
export function planDuration(steps: PacedStep[]): number {
  return steps.reduce((total, step) => total + step.delayMs, 0);
}

function lengthPause(length: number, pacing: PacingSettings): number {
  const cps = pacing.typingSpeedCps > 0 ? pacing.typingSpeedCps : 1;
  return clamp(
    Math.round((1000 * length) / cps),
    Math.max(0, pacing.minDelayMs),
    Math.max(pacing.minDelayMs, pacing.maxDelayMs),
  );
}

function lastTextStep(steps: PacedStep[]): PacedStep | undefined {
  for (let i = steps.length - 1; i >= 0; i--) {
    if (isTextBlock(steps[i].output)) return steps[i];
  }
  return undefined;
}

/**
 * Scale pauses down proportionally when the turn's total exceeds `totalMaxMs`.
 * A six-bubble reply must not take a minute, however slow the configured cps.
 */
function scaleToCeiling(steps: PacedStep[], pacing: PacingSettings): void {
  const ceiling = Math.max(0, pacing.totalMaxMs);
  const total = planDuration(steps);
  if (ceiling <= 0 || total <= ceiling) return;

  const scale = ceiling / total;
  for (const step of steps) {
    // Floor, not round: rounding up can push the scaled total back over the
    // ceiling. The per-step floor can still exceed it when minDelayMs is large
    // relative to the ceiling — that floor wins, deliberately.
    step.delayMs = Math.max(
      Math.max(0, pacing.minDelayMs),
      Math.floor(step.delayMs * scale),
    );
  }
}
