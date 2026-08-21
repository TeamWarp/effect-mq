import { NodeRedis } from "@effect/platform-node"
import { Layer } from "effect"
import type { Redis } from "effect/unstable/persistence"
import * as net from "node:net"

export const redisUrl = process.env.EFFECT_MQ_REDIS_URL ?? "redis://localhost:6380"

/** True when the test Redis (docker compose) is reachable. */
export const redisAvailable = (): Promise<boolean> => {
  const { hostname, port } = new URL(redisUrl)
  return new Promise((resolve) => {
    const socket = net.connect({ host: hostname, port: Number(port || 6379), timeout: 500 })
    socket.once("connect", () => {
      socket.destroy()
      resolve(true)
    })
    socket.once("error", () => resolve(false))
    socket.once("timeout", () => {
      socket.destroy()
      resolve(false)
    })
  })
}

export const redisLive = (): Layer.Layer<Redis.Redis> => NodeRedis.layer({ url: redisUrl }).pipe(Layer.orDie)

let prefixCounter = 0

/** A unique key prefix per store instance — the Redis analogue of fresh tables. */
export const freshPrefix = (): string => `emq-test-${process.pid}-${Date.now()}-${++prefixCounter}`
