import { jobStoreConformance } from "../src/testing/index.ts"
import { MemoryJobStore } from "../src/index.ts"

jobStoreConformance("MemoryJobStore", () => MemoryJobStore.layer)
