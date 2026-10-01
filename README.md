# astro-chat-pacer

Brains answers a turn all at once: one `astro_chat_out` message holding every
block of the reply. A person doesn't talk like that. This service consumes that
message and republishes it to **`astro_chat_paced`, one block at a time**, holding
each block's pause before it goes out — so whatever consumes the paced topic can
forward each message on arrival and the reply lands bubble by bubble.

```
astro_chat_out ──► pacer ──► astro_chat_paced
  1 message            (sleeps)      N messages, one per block
  N blocks                           published on their own beat
```

## What it does to a turn

Input (`astro_chat_out`):

```jsonc
{
  "external_query_ref": "10e3b498-…",
  "external_session_ref": "3b83adc6-…",
  "speaker_agent_id": 1,
  "astrologer_id": 4,            // coming soon; absent today
  "outputs": [
    { "type": "text",  "content": "Haa beta, main ek AI astrology consultant hoon…" },
    { "type": "text",  "content": "Agar aapko kuch specific chizein pata chahiye ho…" },
    { "type": "pills", "items": [ … ] }
  ],
  "decision": "continue"
}
```

Output (`astro_chat_paced`), two messages, the second published ~4s after the first:

```jsonc
{
  "external_query_ref": "10e3b498-…",
  "external_session_ref": "3b83adc6-…",
  "speaker_agent_id": 1,
  "astrologer_id": 4,
  "block_index": 0,
  "block_count": 2,
  "is_last": false,
  "output": { "type": "text", "content": "Haa beta, main ek AI astrology consultant hoon…" },
  "typing_ms": 4800,
  "decision": "continue",
  "published_at": 1759300000000
}
```

```jsonc
{
  …,
  "block_index": 1,
  "is_last": true,
  "output": { "type": "text", "content": "Agar aapko kuch specific chizein pata chahiye ho…" },
  "pills": [ … ],          // the pills block, folded onto the last text block
  "typing_ms": 4000
}
```

Rules:

- one `text` block → one message; blank blocks are dropped
- every `pills` block's items are flattened onto the **last text message**, so
  quick actions arrive with the closing bubble (a pills-only turn publishes them
  alone)
- unknown block types are forwarded untouched, with the minimum pause
- `typing_ms` is **reported, not to be waited on again** — the pause has already
  been held before publishing
- both topics are keyed by `external_session_ref`, so a conversation stays on one
  partition and keeps its order

## Pacing

Pause before a block = `clamp(minDelayMs, maxDelayMs, 1000 × length / typingSpeedCps)`,
plus `initialDelayMs` on the first block. If a turn's pauses sum past
`totalMaxMs` they are scaled down proportionally (each still at least
`minDelayMs`), so a long reading can't take a minute. No jitter — a turn paces
the same way every time.

`typingSpeedCps` is *perceived* characters per second. It is the dial that makes
a pause proportionate to a bubble's length; the clamps are what keep it sane.

> **Per-astrologer pacing is not wired up yet.** Every reply currently paces on
> the platform defaults in `config/config.json`. The lookup lives behind
> `PacingProvider` (`src/pacing/provider.ts`) with `ApiPacingProvider` stubbed —
> drop the endpoint and response body in there and flip `pacing.api.enabled`.

## Two deployables, one codebase

`ROLE` picks what boots. Both roles produce to `astro_chat_paced` and serve
`/health`; only the consumer joins the consumer group.

| image | `ROLE` | what it does |
|---|---|---|
| `Dockerfile.consumer` | `consumer` | reads `astro_chat_out`, paces every turn, publishes each block. The production path — scale by partition count. |
| `Dockerfile.service` | `service` | HTTP API + producer, **no** consumer group (it never steals a partition). Replay and inspection. |

```bash
docker build -f Dockerfile.consumer -t astro-chat-pacer:consumer .
docker build -f Dockerfile.service  -t astro-chat-pacer:service  .
```

### API (service role)

```
GET  /health        → { status, role, consuming, pending }
POST /turns/pace    → body: an astro_chat_out turn, verbatim
```

`POST /turns/pace` paces the turn onto `astro_chat_paced` exactly as the consumer
would — same plan, same pauses, same messages — and returns the plan immediately
(`202`) while the blocks go out on their beats. Add `"dry_run": true` to the body
to get the plan back (`200`) without publishing.

```jsonc
// 202
{
  "accepted": true,
  "external_session_ref": "3b83adc6-…",
  "block_count": 2,
  "total_ms": 8800,
  "typing_speed_cps": 4,
  "blocks": [
    { "block_index": 0, "type": "text", "typing_ms": 4800, "has_pills": false },
    { "block_index": 1, "type": "text", "typing_ms": 4000, "has_pills": true }
  ]
}
```

Bad body → `400` with an error code; a turn with nothing publishable → `422`.

## Layout

| path | what |
|---|---|
| `src/index.ts` | bootstrap + `ROLE` dispatch: config → Kafka → runner → HTTP |
| `src/kafka/kafkaService.ts` | consumer (`astro_chat_out`) + producer (`astro_chat_paced`), dedupe by `external_query_ref` |
| `src/kafka/types.ts` | wire types for both topics |
| `src/pacing/pacing.ts` | the plan: which blocks, in what order, after what pause |
| `src/pacing/provider.ts` | where a turn's pacing config comes from |
| `src/pipeline/paceRunner.ts` | holds the pauses and publishes; serialised per session |
| `src/config/` | `DynamicConfig` — `config/config.json` + `config/<NODE_ENV>/config.json`, deep-merged |
| `src/http/api.ts` | `GET /health`, `POST /turns/pace` |

Playback is detached from the Kafka handler — a turn spans seconds and must not
hold a partition — but **serialised per `external_session_ref`**, so two turns of
one conversation can never interleave their blocks. Different sessions pace
concurrently. On `SIGTERM` the consumer stops first, then in-flight playbacks get
`server.shutdownDrainMs` to finish.

All pods run in **one** consumer group: a turn is paced exactly once.

## Config

`config/config.json` is the base; `config/<NODE_ENV>/config.json` is deep-merged
on top (`NODE_ENV` defaults to `staging`). Env overrides: `KAFKA_BROKERS` (comma
separated), `KAFKA_CLIENT_ID`, `KAFKA_GROUP_ID`, `KAFKA_INPUT_TOPIC`,
`KAFKA_OUTPUT_TOPIC`, `HOST`, `PORT`, `LOG_LEVEL`.

```jsonc
"pacing": {
  "typingSpeedCps": 4,      // perceived chars/sec
  "initialDelayMs": 800,    // opening beat, first block only
  "minDelayMs": 450,
  "maxDelayMs": 4000,       // ceiling on one pause
  "totalMaxMs": 15000,      // ceiling on the whole turn
  "pillsDelayMs": 0,        // only used when pills ride alone
  "api": { "enabled": false, "baseUrl": "", "timeoutMs": 3000, "cacheTtlMs": 300000 }
}
```

## Running

```bash
npm install
npm run dev:consumer    # ts-node, ROLE=consumer
npm run dev:service     # ts-node, ROLE=service
npm run build && npm run start:consumer
npm test                # pacing planner tests, no Kafka needed
npm run typecheck
```

On `SIGTERM` a pod stops the HTTP listener, disconnects the consumer, then keeps
the **producer** up for `server.shutdownDrainMs` so turns mid-playback can finish
publishing their remaining blocks.
