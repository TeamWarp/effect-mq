/**
 * Test kit for effect-mq: the `JobStore` conformance suite every storage
 * driver must pass. Requires `@effect/vitest` (an optional peer dependency).
 *
 * @since 0.1.0
 */
export * from "./conformance.ts"

/**
 * Assert what services enqueue in unit tests, with typed payloads.
 *
 * @since 0.3.2
 */
export * as TestJobStore from "./TestJobStore.ts"
