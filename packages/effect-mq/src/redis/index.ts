/**
 * The Redis-backed `JobStore`, built on `effect/unstable/persistence`'s
 * `Redis` service (provide it via `NodeRedis.layer`, `BunRedis.layer`, or
 * `Redis.make` over any client).
 *
 * @since 0.2.0
 */
export * as RedisJobStore from "./RedisJobStore.ts"
