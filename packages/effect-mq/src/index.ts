/**
 * Effect-native, storage-agnostic background jobs.
 *
 * @since 0.1.0
 */

/**
 * Schema-first job definitions: `Job.make`, `enqueue`, `toLayer`.
 *
 * @since 0.1.0
 */
export * as Job from "./Job.ts"

/**
 * The storage seam: the `JobStore` service, job records and typed errors.
 *
 * @since 0.1.0
 */
export * as JobStore from "./JobStore.ts"

/**
 * The reference in-memory `JobStore` driver (Effect primitives only).
 *
 * @since 0.1.0
 */
export * as MemoryJobStore from "./MemoryJobStore.ts"

/**
 * The worker runtime: `Worker.layer` and handler registration.
 *
 * @since 0.1.0
 */
export * as Worker from "./Worker.ts"
