import assert from "node:assert/strict";
import { PacingSettings } from "../src/config/types";
import { OutputBlock } from "../src/kafka/types";
import { isPillsBlock, planDuration, planTurn } from "../src/pacing/pacing";

const PACING: PacingSettings = {
  typingSpeedCps: 4,
  initialDelayMs: 800,
  minDelayMs: 450,
  maxDelayMs: 4000,
  totalMaxMs: 15000,
  pillsDelayMs: 0,
};

/** The sample turn from astro_chat_out: two text blocks, then a pills block. */
const SAMPLE: OutputBlock[] = [
  {
    type: "text",
    content:
      "Haa beta, main ek AI astrology consultant hoon. Lekin aapke chart ke baare mein sachchai toh main aapke saath share kar raha hoon.",
    subject: null,
  },
  {
    type: "text",
    content:
      "Agar aapko kuch specific chizein pata chahiye ho, toh bataiye — kariyera, sambandh, dosha ya dasha ke baare mein?",
    subject: null,
  },
  {
    type: "pills",
    items: [
      { kind: "send_message", label: "What period am I running now?" },
      { kind: "send_message", label: "Do I have Mangal Dosha?" },
    ],
  },
];

function run(name: string, fn: () => void): void {
  fn();
  console.log(`  ok  ${name}`);
}

run("one step per text block, pills folded onto the last", () => {
  const steps = planTurn(SAMPLE, PACING);
  assert.equal(steps.length, 2);
  assert.equal(steps[0].pills, undefined);
  assert.equal(steps[1].pills?.length, 2);
});

run("first step carries the opening beat", () => {
  const steps = planTurn(SAMPLE, PACING);
  // Both blocks are long enough to hit maxDelayMs; only the first adds the lead.
  assert.equal(steps[0].delayMs, PACING.maxDelayMs + PACING.initialDelayMs);
  assert.equal(steps[1].delayMs, PACING.maxDelayMs);
});

run("a short block paces by length, floored at minDelayMs", () => {
  const steps = planTurn(
    [
      { type: "text", content: "Ji beta." }, // 8 chars / 4 cps = 2000ms
      { type: "text", content: "Ok." }, // 3 chars = 750ms
      { type: "text", content: "." }, // 1 char = 250ms -> floored to 450
    ],
    { ...PACING, initialDelayMs: 0 },
  );
  assert.deepEqual(
    steps.map((s) => s.delayMs),
    [2000, 750, 450],
  );
});

run("blank blocks are dropped", () => {
  const steps = planTurn(
    [
      { type: "text", content: "   " },
      { type: "text", content: "Haan." },
    ],
    PACING,
  );
  assert.equal(steps.length, 1);
});

run("total is scaled down to the ceiling, floored at minDelayMs", () => {
  const long = "a".repeat(400);
  const steps = planTurn(
    [
      { type: "text", content: long },
      { type: "text", content: long },
      { type: "text", content: long },
      { type: "text", content: long },
      { type: "text", content: long },
    ],
    PACING,
  );
  assert.equal(planDuration(steps) <= PACING.totalMaxMs, true);
  assert.equal(
    steps.every((s) => s.delayMs >= PACING.minDelayMs),
    true,
  );
});

run("pills with no text ride on their own", () => {
  const steps = planTurn([SAMPLE[2]], PACING);
  assert.equal(steps.length, 1);
  assert.equal(isPillsBlock(steps[0].output), true);
  assert.equal(steps[0].delayMs, PACING.pillsDelayMs);
});

run("unknown block types are forwarded, not dropped", () => {
  const steps = planTurn(
    [{ type: "carousel", items: [] } as unknown as OutputBlock],
    PACING,
  );
  assert.equal(steps.length, 1);
  assert.equal(steps[0].output.type, "carousel");
});

console.log("\nall pacing tests passed");
