/**
 * Lua scripts backing `RedisJobStore`.
 *
 * Every mutation is one script, so every `JobStore` method is atomic on the
 * server. Two disciplines apply throughout:
 *
 * - **All time comes in via ARGV** (from the Effect `Clock`) — never Redis
 *   `TIME` — so the conformance suite drives this driver under `TestClock`
 *   against a real server.
 * - **Numbers that become strings are formatted with `%.0f`** — Lua 5.1's
 *   `tostring` renders large integers as `1.7e+12`, which would corrupt ids
 *   and stored timestamps.
 *
 * Key layout (all under a configurable prefix, non-cluster):
 *
 * - `p:seq`                     counter for seq + default job ids
 * - `p:job:<id>`                HASH of the job record
 * - `p:attempts:<id>`           LIST of JSON attempt-ledger entries
 * - `p:waiting:<queue>`         ZSET, score `-priority`, member `<seq %016d>:<id>`
 *                               (score = priority desc; member lex = FIFO)
 * - `p:delayed:<queue>`         ZSET, score `runAt`
 * - `p:active`                  ZSET, score `lockExpiresAt`
 * - `p:all`                     ZSET, score `enqueuedAt` (list pagination)
 * - `p:finished:<state>`        ZSET, score `finishedAt` (history TTL)
 * - `p:terminal:<name>:<state>` ZSET, score `finishedAt` (keep pruning)
 * - `p:counts`                  HASH `<queue>|<state>` -> integer
 * - `p:paused`                  SET of paused queues
 * - `p:schedules` / `p:schedule:<key>`  ZSET by nextRunAt + HASH per record
 * - `p:dedupe:<name>\0<key>`   HASH {jobId, expiresAt} + `p:dedupes` index
 *                               ZSET (score = window expiry, +inf = pending)
 *
 * @since 0.2.0
 */
import { Redis } from "effect/unstable/persistence"

/**
 * Shared helpers textually prepended to every script (the `Redis.script`
 * runner has no include mechanism). `ARGV[1]` is always the key prefix.
 */
const HELPERS = `
local prefix = ARGV[1]
local function fmt(x) return string.format("%.0f", x) end
local function jobKey(id) return prefix .. ":job:" .. id end
local function attemptsKey(id) return prefix .. ":attempts:" .. id end
local function waitingKey(queue) return prefix .. ":waiting:" .. queue end
local function delayedKey(queue) return prefix .. ":delayed:" .. queue end
local function terminalKey(name, state) return prefix .. ":terminal:" .. name .. ":" .. state end
-- Waiting order: score = -priority (higher priority first, full number range);
-- FIFO within a priority via lexicographic members "<seq %016d>:<id>". A
-- composite numeric score would clip either priority or seq past float53.
local function waitingMember(seq, id) return string.format("%016.0f", seq) .. ":" .. id end
local function waitingId(member) return string.sub(member, 18) end
local function addWaiting(queue, priority, seq, id)
  redis.call("ZADD", waitingKey(queue), -priority, waitingMember(seq, id))
end
local function remWaiting(queue, id)
  local seq = tonumber(redis.call("HGET", jobKey(id), "seq")) or 0
  redis.call("ZREM", waitingKey(queue), waitingMember(seq, id))
end
local function countsAdd(queue, state, n)
  redis.call("HINCRBY", prefix .. ":counts", queue .. "|" .. state, n)
end
-- One ledger entry. startedAt/finishedAt/exitJson are strings ("" = absent).
local function appendAttempt(id, outcome, startedAt, finishedAt, exitJson)
  local n = redis.call("LLEN", attemptsKey(id)) + 1
  local started = (startedAt == nil or startedAt == "") and "null" or startedAt
  -- The exit key is OMITTED (not null) when absent, so a legitimate encoded
  -- null exit stays distinguishable on the read side.
  local ex = (exitJson == nil or exitJson == "") and "" or (',"exit":' .. exitJson)
  redis.call("RPUSH", attemptsKey(id),
    '{"attempt":' .. n .. ',"startedAt":' .. started .. ',"finishedAt":' .. finishedAt ..
    ',"outcome":"' .. outcome .. '"' .. ex .. '}')
end
-- Remove a job and every index entry that references it.
local function deleteJob(id)
  local jk = jobKey(id)
  local queue = redis.call("HGET", jk, "queue")
  if not queue then return end
  local state = redis.call("HGET", jk, "state")
  local name = redis.call("HGET", jk, "name")
  countsAdd(queue, state, -1)
  redis.call("ZREM", prefix .. ":all", id)
  redis.call("ZREM", prefix .. ":finished:" .. state, id)
  redis.call("ZREM", prefix .. ":active", id)
  redis.call("ZREM", terminalKey(name, state), id)
  remWaiting(queue, id)
  redis.call("ZREM", delayedKey(queue), id)
  redis.call("DEL", jk, attemptsKey(id))
end
-- Terminal retention for one name+state group. Correctness over speed: the
-- count rule sorts the group by (finishedAt DESC, seq DESC) in Lua because a
-- zset score alone cannot carry the seq tie-break exactly.
local function applyKeep(name, state, keepJson, now)
  if keepJson == nil or keepJson == "" then return end
  local ok, decoded = pcall(cjson.decode, keepJson)
  if not ok or type(decoded) ~= "table" then return end
  -- Policies are split per terminal state; rows persisted by 0.2.x carry the
  -- flat {count, ageMs} shape and apply to every state.
  local keep = decoded[state]
  if type(keep) ~= "table" then
    if decoded.completed == nil and decoded.failed == nil and decoded.cancelled == nil
      and (decoded.count ~= nil or decoded.ageMs ~= nil)
    then
      keep = decoded
    else
      return
    end
  end
  local tkey = terminalKey(name, state)
  if keep.ageMs ~= nil then
    local old = redis.call("ZRANGEBYSCORE", tkey, "-inf", now - keep.ageMs)
    for i = 1, #old do deleteJob(old[i]) end
  end
  -- Floor + clamp: a fractional/negative count must degrade like the memory
  -- driver's slice(), never error mid-script (writes before an error stick).
  local count = keep.count ~= nil and math.floor(math.max(0, tonumber(keep.count) or 0)) or nil
  if count ~= nil and redis.call("ZCARD", tkey) > count then
    local members = redis.call("ZRANGE", tkey, 0, -1, "WITHSCORES")
    local arr = {}
    for i = 1, #members, 2 do
      arr[#arr + 1] = {
        id = members[i],
        fa = tonumber(members[i + 1]),
        seq = tonumber(redis.call("HGET", jobKey(members[i]), "seq")) or 0
      }
    end
    table.sort(arr, function(a, b)
      if a.fa ~= b.fa then return a.fa > b.fa end
      return a.seq > b.seq
    end)
    for i = count + 1, #arr do deleteJob(arr[i].id) end
  end
end
local function dedupeStoreKey(name, key) return prefix .. ":dedupe:" .. name .. "\0" .. key end
-- A job leaving the pending states frees its pending-mode dedup entry; live
-- throttle windows deliberately outlast the job.
local function releaseDedupe(name, dkey, jobId, now)
  if dkey == nil or dkey == false or dkey == "" then return end
  local sk = dedupeStoreKey(name, dkey)
  if redis.call("HGET", sk, "jobId") == jobId then
    local exp = redis.call("HGET", sk, "expiresAt")
    if exp == "" or tonumber(exp) <= now then
      redis.call("DEL", sk)
      redis.call("ZREM", prefix .. ":dedupes", name .. "\0" .. dkey)
    end
  end
end
-- Move an active job (whose lock bookkeeping was already cleared by the
-- caller) to the terminal cancelled state.
local function finishCancelled(id, queue, name, startedAt, now, nowStr)
  local jk = jobKey(id)
  redis.call("HSET", jk, "state", "cancelled", "finishedAt", nowStr, "cancelRequested", "0",
    "lockToken", "", "lockExpiresAt", "")
  countsAdd(queue, "active", -1)
  countsAdd(queue, "cancelled", 1)
  redis.call("ZADD", prefix .. ":finished:cancelled", now, id)
  redis.call("ZADD", terminalKey(name, "cancelled"), now, id)
  appendAttempt(id, "cancelled", startedAt, nowStr, "")
  releaseDedupe(name, redis.call("HGET", jk, "dedupeKey"), id, now)
  applyKeep(name, "cancelled", redis.call("HGET", jk, "keep"), now)
end
`

/**
 * enqueue(prefix, idMode, id, name, queue, payloadJson, metadataJson,
 *         priority, attemptsMax, backoffJson, keepJson, timeoutMs, delayMs, now)
 * idMode: "user" (dedup no-op), "generated" (collision -> retry sentinel),
 * "auto" (j-<seq>, in-script collision loop).
 */
export const enqueue = Redis.script(
  (
    prefix: string,
    idMode: string,
    id: string,
    name: string,
    queue: string,
    payloadJson: string,
    metadataJson: string,
    priority: number,
    attemptsMax: number,
    backoffJson: string,
    keepJson: string,
    timeoutMs: string,
    delayMs: number,
    now: number,
    dedupeKey: string,
    dedupeTtlMs: string,
    dedupeExtend: string,
    dedupeReplace: string,
    traceJson: string
  ) => [
    prefix,
    idMode,
    id,
    name,
    queue,
    payloadJson,
    metadataJson,
    priority,
    attemptsMax,
    backoffJson,
    keepJson,
    timeoutMs,
    delayMs,
    now,
    dedupeKey,
    dedupeTtlMs,
    dedupeExtend,
    dedupeReplace,
    traceJson
  ],
  {
    numberOfKeys: 0,
    lua: `${HELPERS}
local idMode, id = ARGV[2], ARGV[3]
local queue = ARGV[5]
local nowStr = ARGV[14]
local now = tonumber(nowStr)
local delayMs = tonumber(ARGV[13])
if idMode == "user" or idMode == "generated" then
  if redis.call("EXISTS", jobKey(id)) == 1 then
    if idMode == "user" then return '{"duplicate":true,"id":' .. cjson.encode(id) .. '}' end
    return '{"collision":true}'
  end
end
-- Dedup decision tree: replace-while-delayed, throttle window, pending dedup.
local dKey = ARGV[15]
local name = ARGV[4]
if dKey ~= "" then
  local sk = dedupeStoreKey(name, dKey)
  local entryJob = redis.call("HGET", sk, "jobId")
  if entryJob then
    local expStr = redis.call("HGET", sk, "expiresAt")
    local windowLive = expStr ~= "" and tonumber(expStr) > now
    local keyedState = redis.call("HGET", jobKey(entryJob), "state")
    local function bumpWindow()
      if ARGV[17] == "1" and ARGV[16] ~= "" then
        local windowEnd = now + tonumber(ARGV[16])
        redis.call("HSET", sk, "expiresAt", fmt(windowEnd))
        redis.call("ZADD", prefix .. ":dedupes", windowEnd, name .. "\0" .. dKey)
      end
    end
    -- Latest-wins while the keyed job is still delayed.
    if ARGV[18] == "1" and keyedState == "delayed" then
      local kjk = jobKey(entryJob)
      local newRunAt = now + delayMs
      redis.call("HSET", kjk, "payload", ARGV[6], "metadata", ARGV[7], "priority", ARGV[8],
        "attemptsMax", ARGV[9], "backoff", ARGV[10], "keep", ARGV[11], "timeoutMs", ARGV[12],
        "trace", ARGV[19], "runAt", fmt(newRunAt))
      local keyedQueue = redis.call("HGET", kjk, "queue")
      redis.call("ZADD", delayedKey(keyedQueue), newRunAt, entryJob)
      -- A landed replace re-arms the ttl window.
      if ARGV[16] ~= "" then
        local windowEnd = now + tonumber(ARGV[16])
        redis.call("HSET", sk, "expiresAt", fmt(windowEnd))
        redis.call("ZADD", prefix .. ":dedupes", windowEnd, name .. "\0" .. dKey)
      end
      return '{"id":' .. cjson.encode(entryJob) .. ',"duplicate":true,"wake":true,"queue":' .. cjson.encode(keyedQueue) .. '}'
    end
    if windowLive then
      bumpWindow()
      return '{"id":' .. cjson.encode(entryJob) .. ',"duplicate":true}'
    end
    local pending = keyedState ~= false and keyedState ~= "completed"
      and keyedState ~= "failed" and keyedState ~= "cancelled"
    if expStr == "" and pending then
      return '{"id":' .. cjson.encode(entryJob) .. ',"duplicate":true}'
    end
    -- Dead entry: the new job takes over the key below.
  end
end
if idMode == "auto" then
  id = ""
  for i = 1, 5 do
    local candidate = "j-" .. fmt(redis.call("INCR", prefix .. ":seq"))
    if redis.call("EXISTS", jobKey(candidate)) == 0 then
      id = candidate
      break
    end
  end
  if id == "" then return '{"error":"id"}' end
end
local seq = redis.call("INCR", prefix .. ":seq")
local state = delayMs > 0 and "delayed" or "waiting"
local runAt = now + delayMs
redis.call("HSET", jobKey(id),
  "id", id, "name", ARGV[4], "queue", queue,
  "payload", ARGV[6], "metadata", ARGV[7], "state", state,
  "priority", ARGV[8], "attemptsMax", ARGV[9], "attemptsMade", "0", "stalledCount", "0",
  "backoff", ARGV[10], "keep", ARGV[11], "timeoutMs", ARGV[12],
  "cancelRequested", "0", "dedupeKey", dKey, "trace", ARGV[19], "runAt", fmt(runAt), "enqueuedAt", nowStr,
  "processedAt", "", "finishedAt", "", "exit", "", "failedReason", "",
  "lockToken", "", "lockExpiresAt", "", "seq", fmt(seq))
redis.call("ZADD", prefix .. ":all", now, id)
if state == "waiting" then
  addWaiting(queue, tonumber(ARGV[8]), seq, id)
else
  redis.call("ZADD", delayedKey(queue), runAt, id)
end
countsAdd(queue, state, 1)
if dKey ~= "" then
  local sk = dedupeStoreKey(name, dKey)
  redis.call("DEL", sk)
  redis.call("HSET", sk, "jobId", id,
    "expiresAt", ARGV[16] == "" and "" or fmt(now + tonumber(ARGV[16])))
  redis.call("ZADD", prefix .. ":dedupes",
    ARGV[16] == "" and "inf" or tostring(now + tonumber(ARGV[16])), name .. "\0" .. dKey)
end
-- Wake on EVERY insert (delayed too): idle workers must re-claim to learn
-- the new nextRunAt, exactly like the memory and Postgres drivers.
return '{"id":' .. cjson.encode(id) .. ',"duplicate":false,"wake":true}'
`
  }
).withReturnType<string>()

/**
 * claim(prefix, queue, namesJson, token, lockDurationMs, now)
 * Promotes due delayed jobs first (also while paused), then claims the best
 * waiting job whose name matches. Returns the claimed record (HGETALL pairs)
 * or an Empty result with the earliest matching delayed runAt.
 */
export const claim = Redis.script(
  (prefix: string, queue: string, namesJson: string, token: string, lockDurationMs: number, now: number) => [
    prefix,
    queue,
    namesJson,
    token,
    lockDurationMs,
    now
  ],
  {
    numberOfKeys: 0,
    lua: `${HELPERS}
local queue = ARGV[2]
local nameSet = {}
for _, n in ipairs(cjson.decode(ARGV[3])) do nameSet[n] = true end
local token = ARGV[4]
local lockDurationMs = tonumber(ARGV[5])
local nowStr = ARGV[6]
local now = tonumber(nowStr)

-- Promote due delayed jobs (state change is visible even while paused).
local due = redis.call("ZRANGEBYSCORE", delayedKey(queue), "-inf", now)
for i = 1, #due do
  local id = due[i]
  local jk = jobKey(id)
  redis.call("ZREM", delayedKey(queue), id)
  local priority = tonumber(redis.call("HGET", jk, "priority")) or 0
  local seq = tonumber(redis.call("HGET", jk, "seq")) or 0
  redis.call("HSET", jk, "state", "waiting")
  addWaiting(queue, priority, seq, id)
  countsAdd(queue, "delayed", -1)
  countsAdd(queue, "waiting", 1)
end

-- Earliest matching delayed job: how long an idle worker may sleep.
local nextRunAt = nil
local delayed = redis.call("ZRANGE", delayedKey(queue), 0, -1, "WITHSCORES")
for i = 1, #delayed, 2 do
  if nameSet[redis.call("HGET", jobKey(delayed[i]), "name")] then
    nextRunAt = tonumber(delayed[i + 1])
    break
  end
end

if redis.call("SISMEMBER", prefix .. ":paused", queue) == 1 then
  return cjson.encode({ empty = true, nextRunAt = nextRunAt })
end

local offset = 0
while true do
  local batch = redis.call("ZRANGE", waitingKey(queue), offset, offset + 99)
  if #batch == 0 then break end
  for i = 1, #batch do
    local id = waitingId(batch[i])
    local jk = jobKey(id)
    if nameSet[redis.call("HGET", jk, "name")] then
      redis.call("ZREM", waitingKey(queue), batch[i])
      redis.call("HSET", jk, "state", "active", "lockToken", token,
        "lockExpiresAt", fmt(now + lockDurationMs), "processedAt", nowStr)
      redis.call("ZADD", prefix .. ":active", now + lockDurationMs, id)
      countsAdd(queue, "waiting", -1)
      countsAdd(queue, "active", 1)
      return cjson.encode({ job = redis.call("HGETALL", jk) })
    end
  end
  offset = offset + 100
end
return cjson.encode({ empty = true, nextRunAt = nextRunAt })
`
  }
).withReturnType<string>()

/**
 * ack(prefix, id, token, outcomeTag, exitJson, delayMs, now)
 * Token-guarded. Retry on a cancel-requested job finishes it as cancelled
 * (cancellation wins over revival, mirroring release/recoverStalled).
 */
export const ack = Redis.script(
  (prefix: string, id: string, token: string, outcomeTag: string, exitJson: string, delayMs: number, now: number) => [
    prefix,
    id,
    token,
    outcomeTag,
    exitJson,
    delayMs,
    now
  ],
  {
    numberOfKeys: 0,
    lua: `${HELPERS}
local id = ARGV[2]
local jk = jobKey(id)
if redis.call("EXISTS", jk) == 0 then return '{"error":"notfound"}' end
if redis.call("HGET", jk, "state") ~= "active" or redis.call("HGET", jk, "lockToken") ~= ARGV[3] then
  return '{"error":"locklost"}'
end
local tag = ARGV[4]
local exitJson = ARGV[5]
local delayMs = tonumber(ARGV[6])
local nowStr = ARGV[7]
local now = tonumber(nowStr)
local queue = redis.call("HGET", jk, "queue")
local name = redis.call("HGET", jk, "name")
local startedAt = redis.call("HGET", jk, "processedAt")
local cancelRequested = redis.call("HGET", jk, "cancelRequested") == "1"

redis.call("HINCRBY", jk, "attemptsMade", 1)
redis.call("ZREM", prefix .. ":active", id)

local function finish(newState, storeExit, outcome, ledgerExit)
  redis.call("HSET", jk, "state", newState, "finishedAt", nowStr, "cancelRequested", "0",
    "lockToken", "", "lockExpiresAt", "")
  if storeExit ~= nil then redis.call("HSET", jk, "exit", storeExit) end
  countsAdd(queue, "active", -1)
  countsAdd(queue, newState, 1)
  redis.call("ZADD", prefix .. ":finished:" .. newState, now, id)
  redis.call("ZADD", terminalKey(name, newState), now, id)
  appendAttempt(id, outcome, startedAt, nowStr, ledgerExit)
  releaseDedupe(name, redis.call("HGET", jk, "dedupeKey"), id, now)
  applyKeep(name, newState, redis.call("HGET", jk, "keep"), now)
end

if tag == "Complete" then
  finish("completed", exitJson, "completed", exitJson)
elseif tag == "Fail" then
  finish("failed", exitJson, "failed", exitJson)
elseif tag == "Cancelled" then
  finish("cancelled", nil, "cancelled", "")
elseif cancelRequested then
  finish("cancelled", nil, "cancelled", "")
else
  appendAttempt(id, "retried", startedAt, nowStr, exitJson)
  local seq = redis.call("INCR", prefix .. ":seq")
  local runAt = now + delayMs
  local state = delayMs > 0 and "delayed" or "waiting"
  redis.call("HSET", jk, "state", state, "seq", fmt(seq), "runAt", fmt(runAt),
    "lockToken", "", "lockExpiresAt", "")
  countsAdd(queue, "active", -1)
  countsAdd(queue, state, 1)
  if state == "waiting" then
    local priority = tonumber(redis.call("HGET", jk, "priority")) or 0
    addWaiting(queue, priority, seq, id)
  else
    redis.call("ZADD", delayedKey(queue), runAt, id)
  end
  return '{"ok":true,"wake":true,"queue":' .. cjson.encode(queue) .. '}'
end
return '{"ok":true}'
`
  }
).withReturnType<string>()

/**
 * release(prefix, id, token, now) — hand the job back without consuming an
 * attempt; a pending cancel wins and finishes the job instead.
 */
export const release = Redis.script(
  (prefix: string, id: string, token: string, now: number) => [prefix, id, token, now],
  {
    numberOfKeys: 0,
    lua: `${HELPERS}
local id = ARGV[2]
local jk = jobKey(id)
if redis.call("EXISTS", jk) == 0 then return '{"error":"notfound"}' end
if redis.call("HGET", jk, "state") ~= "active" or redis.call("HGET", jk, "lockToken") ~= ARGV[3] then
  return '{"error":"locklost"}'
end
local nowStr = ARGV[4]
local now = tonumber(nowStr)
local queue = redis.call("HGET", jk, "queue")
redis.call("ZREM", prefix .. ":active", id)
if redis.call("HGET", jk, "cancelRequested") == "1" then
  finishCancelled(id, queue, redis.call("HGET", jk, "name"), redis.call("HGET", jk, "processedAt"), now, nowStr)
  return '{"ok":true}'
end
local seq = tonumber(redis.call("HGET", jk, "seq")) or 0
local priority = tonumber(redis.call("HGET", jk, "priority")) or 0
redis.call("HSET", jk, "state", "waiting", "lockToken", "", "lockExpiresAt", "")
addWaiting(queue, priority, seq, id)
countsAdd(queue, "active", -1)
countsAdd(queue, "waiting", 1)
return '{"ok":true,"wake":true,"queue":' .. cjson.encode(queue) .. '}'
`
  }
).withReturnType<string>()

/**
 * extendLocks(prefix, locksJson, durationMs, now) -> { lost, cancel }
 * Cancel-requested locks are reported, not extended.
 */
export const extendLocks = Redis.script(
  (prefix: string, locksJson: string, durationMs: number, now: number) => [prefix, locksJson, durationMs, now],
  {
    numberOfKeys: 0,
    lua: `${HELPERS}
local locks = cjson.decode(ARGV[2])
local durationMs = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local lost, cancel = {}, {}
for _, lock in ipairs(locks) do
  local jk = jobKey(lock.id)
  if redis.call("HGET", jk, "state") ~= "active" or redis.call("HGET", jk, "lockToken") ~= lock.token then
    lost[#lost + 1] = lock.id
  elseif redis.call("HGET", jk, "cancelRequested") == "1" then
    cancel[#cancel + 1] = lock.id
  else
    redis.call("HSET", jk, "lockExpiresAt", fmt(now + durationMs))
    redis.call("ZADD", prefix .. ":active", now + durationMs, lock.id)
  end
end
return cjson.encode({ lost = lost, cancel = cancel })
`
  }
).withReturnType<string>()

/**
 * recoverStalled(prefix, maxStalledCount, now) -> recovered [{id, failed}]
 * A pending cancel finishes the job as cancelled (not reported as recovered).
 */
export const recoverStalled = Redis.script(
  (prefix: string, maxStalledCount: number, now: number) => [prefix, maxStalledCount, now],
  {
    numberOfKeys: 0,
    lua: `${HELPERS}
local maxStalledCount = tonumber(ARGV[2])
local nowStr = ARGV[3]
local now = tonumber(nowStr)
local expired = redis.call("ZRANGEBYSCORE", prefix .. ":active", "-inf", now)
local recovered = {}
for _, id in ipairs(expired) do
  local jk = jobKey(id)
  if redis.call("HGET", jk, "state") == "active" then
    local queue = redis.call("HGET", jk, "queue")
    local name = redis.call("HGET", jk, "name")
    local startedAt = redis.call("HGET", jk, "processedAt")
    redis.call("ZREM", prefix .. ":active", id)
    if redis.call("HGET", jk, "cancelRequested") == "1" then
      finishCancelled(id, queue, name, startedAt, now, nowStr)
    else
      local stalled = (tonumber(redis.call("HGET", jk, "stalledCount")) or 0) + 1
      redis.call("HSET", jk, "stalledCount", fmt(stalled), "lockToken", "", "lockExpiresAt", "")
      appendAttempt(id, "stalled", startedAt, nowStr, "")
      if stalled > maxStalledCount then
        redis.call("HSET", jk, "state", "failed", "finishedAt", nowStr,
          "failedReason", "job stalled more than allowable limit")
        countsAdd(queue, "active", -1)
        countsAdd(queue, "failed", 1)
        redis.call("ZADD", prefix .. ":finished:failed", now, id)
        redis.call("ZADD", terminalKey(name, "failed"), now, id)
        releaseDedupe(name, redis.call("HGET", jk, "dedupeKey"), id, now)
        recovered[#recovered + 1] = { id = id, failed = true }
      else
        local seq = tonumber(redis.call("HGET", jk, "seq")) or 0
        local priority = tonumber(redis.call("HGET", jk, "priority")) or 0
        redis.call("HSET", jk, "state", "waiting")
        addWaiting(queue, priority, seq, id)
        countsAdd(queue, "active", -1)
        countsAdd(queue, "waiting", 1)
        recovered[#recovered + 1] = { id = id, failed = false }
      end
    end
  end
end
if #recovered == 0 then return "[]" end
return cjson.encode(recovered)
`
  }
).withReturnType<string>()

/** getJob(prefix, id) -> HGETALL pairs (empty array when missing). */
export const getJob = Redis.script(
  (prefix: string, id: string) => [prefix, id],
  {
    numberOfKeys: 0,
    lua: `${HELPERS}
local record = redis.call("HGETALL", jobKey(ARGV[2]))
if #record == 0 then return "[]" end
return cjson.encode(record)
`
  }
).withReturnType<string>()

/**
 * list(prefix, filtersJson, cursor, limit)
 * Keyset pagination over p:all, newest first (enqueuedAt DESC, id DESC).
 */
export const list = Redis.script(
  (prefix: string, filtersJson: string, cursor: string, limit: number) => [prefix, filtersJson, cursor, limit],
  {
    numberOfKeys: 0,
    lua: `${HELPERS}
-- A filter that Redis's cjson cannot decode (e.g. lone-surrogate escapes)
-- degrades to an empty page instead of a script error.
local okFilters, filters = pcall(cjson.decode, ARGV[2])
if not okFilters then return '{"items":[],"more":false}' end
local stateSet = nil
if filters.states ~= nil then
  stateSet = {}
  for _, s in ipairs(filters.states) do stateSet[s] = true end
end
local cursorAt, cursorId = nil, nil
if ARGV[3] ~= "" then
  local split = string.find(ARGV[3], ":", 1, true)
  if split ~= nil then
    cursorAt = tonumber(string.sub(ARGV[3], 1, split - 1))
    cursorId = string.sub(ARGV[3], split + 1)
  end
  if cursorAt == nil then cursorId = nil end
end
local limit = tonumber(ARGV[4])
local items = {}
local moreMatches = false
local max = cursorAt == nil and "+inf" or fmt(cursorAt)
local offset = 0
while true do
  local batch = redis.call("ZREVRANGEBYSCORE", prefix .. ":all", max, "-inf", "WITHSCORES", "LIMIT", offset, 100)
  if #batch == 0 then break end
  for i = 1, #batch, 2 do
    local id = batch[i]
    local at = tonumber(batch[i + 1])
    -- Skip up to and including the cursor position within its score.
    if cursorAt == nil or at < cursorAt or (at == cursorAt and id < cursorId) then
      local jk = jobKey(id)
      local matches = true
      if filters.queue ~= nil and redis.call("HGET", jk, "queue") ~= filters.queue then matches = false end
      if matches and filters.name ~= nil and redis.call("HGET", jk, "name") ~= filters.name then matches = false end
      if matches and stateSet ~= nil and not stateSet[redis.call("HGET", jk, "state")] then matches = false end
      if matches and filters.metadata ~= nil then
        local okMeta, meta = pcall(cjson.decode, redis.call("HGET", jk, "metadata"))
        if not okMeta then
          matches = false
        else
          for k, v in pairs(filters.metadata) do
            if meta[k] ~= v then
              matches = false
              break
            end
          end
        end
      end
      if matches then
        if #items >= limit then
          moreMatches = true
          break
        end
        items[#items + 1] = redis.call("HGETALL", jk)
      end
    end
  end
  if moreMatches then break end
  offset = offset + 100
end
if #items == 0 then return '{"items":[],"more":false}' end
return cjson.encode({ items = items, more = moreMatches })
`
  }
).withReturnType<string>()

/** counts(prefix) -> HGETALL pairs of p:counts. */
export const counts = Redis.script(
  (prefix: string) => [prefix],
  {
    numberOfKeys: 0,
    lua: `${HELPERS}
local pairs_ = redis.call("HGETALL", prefix .. ":counts")
if #pairs_ == 0 then return "[]" end
return cjson.encode(pairs_)
`
  }
).withReturnType<string>()

/** remove(prefix, id) -> removed boolean (active jobs are refused). */
export const remove = Redis.script(
  (prefix: string, id: string) => [prefix, id],
  {
    numberOfKeys: 0,
    lua: `${HELPERS}
local id = ARGV[2]
local jk = jobKey(id)
if redis.call("EXISTS", jk) == 0 or redis.call("HGET", jk, "state") == "active" then
  return "0"
end
deleteJob(id)
return "1"
`
  }
).withReturnType<string>()

/** retry(prefix, id, now) — failed -> waiting with a fresh budget. */
export const retry = Redis.script(
  (prefix: string, id: string, now: number) => [prefix, id, now],
  {
    numberOfKeys: 0,
    lua: `${HELPERS}
local id = ARGV[2]
local jk = jobKey(id)
if redis.call("EXISTS", jk) == 0 then return '{"error":"notfound"}' end
local state = redis.call("HGET", jk, "state")
if state ~= "failed" then return '{"error":"state","state":' .. cjson.encode(state) .. '}' end
local nowStr = ARGV[3]
local now = tonumber(nowStr)
local queue = redis.call("HGET", jk, "queue")
local name = redis.call("HGET", jk, "name")
local seq = redis.call("INCR", prefix .. ":seq")
redis.call("HSET", jk, "state", "waiting", "attemptsMade", "0", "stalledCount", "0",
  "cancelRequested", "0", "exit", "", "failedReason", "", "finishedAt", "",
  "processedAt", "", "runAt", nowStr, "seq", fmt(seq))
redis.call("ZREM", prefix .. ":finished:failed", id)
redis.call("ZREM", terminalKey(name, "failed"), id)
local priority = tonumber(redis.call("HGET", jk, "priority")) or 0
addWaiting(queue, priority, seq, id)
countsAdd(queue, "failed", -1)
countsAdd(queue, "waiting", 1)
return '{"ok":true,"queue":' .. cjson.encode(queue) .. '}'
`
  }
).withReturnType<string>()

/**
 * cancel(prefix, id, now) — waiting/delayed become terminal; active gets the
 * cancel-request flag; terminal states are refused.
 */
export const cancel = Redis.script(
  (prefix: string, id: string, now: number) => [prefix, id, now],
  {
    numberOfKeys: 0,
    lua: `${HELPERS}
local id = ARGV[2]
local jk = jobKey(id)
if redis.call("EXISTS", jk) == 0 then return '{"error":"notfound"}' end
local state = redis.call("HGET", jk, "state")
if state == "active" then
  redis.call("HSET", jk, "cancelRequested", "1")
  return '{"ok":true}'
end
if state ~= "waiting" and state ~= "delayed" then
  return '{"error":"state","state":' .. cjson.encode(state) .. '}'
end
local nowStr = ARGV[3]
local now = tonumber(nowStr)
local queue = redis.call("HGET", jk, "queue")
local name = redis.call("HGET", jk, "name")
remWaiting(queue, id)
redis.call("ZREM", delayedKey(queue), id)
redis.call("HSET", jk, "state", "cancelled", "finishedAt", nowStr, "cancelRequested", "0")
countsAdd(queue, state, -1)
countsAdd(queue, "cancelled", 1)
redis.call("ZADD", prefix .. ":finished:cancelled", now, id)
redis.call("ZADD", terminalKey(name, "cancelled"), now, id)
appendAttempt(id, "cancelled", redis.call("HGET", jk, "processedAt"), nowStr, "")
releaseDedupe(name, redis.call("HGET", jk, "dedupeKey"), id, now)
applyKeep(name, "cancelled", redis.call("HGET", jk, "keep"), now)
return '{"ok":true}'
`
  }
).withReturnType<string>()

/** promote(prefix, id, now) — delayed -> waiting now. */
export const promote = Redis.script(
  (prefix: string, id: string, now: number) => [prefix, id, now],
  {
    numberOfKeys: 0,
    lua: `${HELPERS}
local id = ARGV[2]
local jk = jobKey(id)
if redis.call("EXISTS", jk) == 0 then return '{"error":"notfound"}' end
local state = redis.call("HGET", jk, "state")
if state ~= "delayed" then return '{"error":"state","state":' .. cjson.encode(state) .. '}' end
local queue = redis.call("HGET", jk, "queue")
local seq = tonumber(redis.call("HGET", jk, "seq")) or 0
local priority = tonumber(redis.call("HGET", jk, "priority")) or 0
redis.call("ZREM", delayedKey(queue), id)
redis.call("HSET", jk, "state", "waiting", "runAt", ARGV[3])
addWaiting(queue, priority, seq, id)
countsAdd(queue, "delayed", -1)
countsAdd(queue, "waiting", 1)
return '{"ok":true,"queue":' .. cjson.encode(queue) .. '}'
`
  }
).withReturnType<string>()

/**
 * upsertSchedule(prefix, key, jobName, queue, cron, tz, everyMs, payloadJson,
 *                metadataJson, priority, attemptsMax, backoffJson, keepJson,
 *                timeoutMs, nextRunAt)
 * Every field arrives pre-encoded from TS and is stored VERBATIM — routing a
 * record through cjson would corrupt high-precision numbers (14 significant
 * digits) and empty arrays ({}). An unchanged cadence (cron/tz/everyMs)
 * preserves the stored nextRunAt.
 */
export const upsertSchedule = Redis.script(
  (
    prefix: string,
    key: string,
    jobName: string,
    queue: string,
    cron: string,
    tz: string,
    everyMs: string,
    payloadJson: string,
    metadataJson: string,
    priority: string,
    attemptsMax: string,
    backoffJson: string,
    keepJson: string,
    timeoutMs: string,
    nextRunAt: number
  ) => [
    prefix,
    key,
    jobName,
    queue,
    cron,
    tz,
    everyMs,
    payloadJson,
    metadataJson,
    priority,
    attemptsMax,
    backoffJson,
    keepJson,
    timeoutMs,
    nextRunAt
  ],
  {
    numberOfKeys: 0,
    lua: `${HELPERS}
local key = ARGV[2]
local sk = prefix .. ":schedule:" .. key
local nextRunAt = tonumber(ARGV[15])
local prevCron = redis.call("HGET", sk, "cron")
if prevCron ~= false
  and prevCron == ARGV[5]
  and redis.call("HGET", sk, "tz") == ARGV[6]
  and redis.call("HGET", sk, "everyMs") == ARGV[7]
then
  nextRunAt = tonumber(redis.call("HGET", sk, "nextRunAt")) or nextRunAt
end
redis.call("DEL", sk)
redis.call("HSET", sk,
  "key", key, "jobName", ARGV[3], "queue", ARGV[4],
  "cron", ARGV[5], "tz", ARGV[6], "everyMs", ARGV[7],
  "payload", ARGV[8], "metadata", ARGV[9],
  "priority", ARGV[10], "attemptsMax", ARGV[11],
  "backoff", ARGV[12], "keep", ARGV[13], "timeoutMs", ARGV[14],
  "nextRunAt", fmt(nextRunAt))
redis.call("ZADD", prefix .. ":schedules", nextRunAt, key)
return '{"ok":true}'
`
  }
).withReturnType<string>()

/** removeSchedule(prefix, key) -> existed boolean. */
export const removeSchedule = Redis.script(
  (prefix: string, key: string) => [prefix, key],
  {
    numberOfKeys: 0,
    lua: `${HELPERS}
local removed = redis.call("ZREM", prefix .. ":schedules", ARGV[2])
redis.call("DEL", prefix .. ":schedule:" .. ARGV[2])
return tostring(removed)
`
  }
).withReturnType<string>()

/** listSchedules(prefix, filtersJson) ordered by nextRunAt ascending. */
export const listSchedules = Redis.script(
  (prefix: string, filtersJson: string) => [prefix, filtersJson],
  {
    numberOfKeys: 0,
    lua: `${HELPERS}
local filters = cjson.decode(ARGV[2])
local keys = redis.call("ZRANGE", prefix .. ":schedules", 0, -1)
local out = {}
for _, key in ipairs(keys) do
  local record = redis.call("HGETALL", prefix .. ":schedule:" .. key)
  local byName = {}
  for i = 1, #record, 2 do byName[record[i]] = record[i + 1] end
  local matches = true
  if filters.jobName ~= nil and byName.jobName ~= filters.jobName then matches = false end
  if matches and filters.queue ~= nil and byName.queue ~= filters.queue then matches = false end
  if matches then out[#out + 1] = record end
end
if #out == 0 then return "[]" end
return cjson.encode(out)
`
  }
).withReturnType<string>()

/** dueSchedules(prefix, now) ordered by nextRunAt ascending. */
export const dueSchedules = Redis.script(
  (prefix: string, now: number) => [prefix, now],
  {
    numberOfKeys: 0,
    lua: `${HELPERS}
local keys = redis.call("ZRANGEBYSCORE", prefix .. ":schedules", "-inf", tonumber(ARGV[2]))
local out = {}
for _, key in ipairs(keys) do
  out[#out + 1] = redis.call("HGETALL", prefix .. ":schedule:" .. key)
end
if #out == 0 then return "[]" end
return cjson.encode(out)
`
  }
).withReturnType<string>()

/** advanceSchedule(prefix, key, expectedRunAt, nextRunAt) — conditional CAS. */
export const advanceSchedule = Redis.script(
  (prefix: string, key: string, expectedRunAt: number, nextRunAt: number) => [prefix, key, expectedRunAt, nextRunAt],
  {
    numberOfKeys: 0,
    lua: `${HELPERS}
local key = ARGV[2]
local sk = prefix .. ":schedule:" .. key
local current = redis.call("HGET", sk, "nextRunAt")
if current == false or tonumber(current) ~= tonumber(ARGV[3]) then return "0" end
redis.call("HSET", sk, "nextRunAt", fmt(tonumber(ARGV[4])))
redis.call("ZADD", prefix .. ":schedules", tonumber(ARGV[4]), key)
return "1"
`
  }
).withReturnType<string>()

/**
 * sweepState(prefix, state, ttlMs, limit, offset, now) -> {scanned, deleted}
 * One bounded page over a terminal state's finished zset, deleting rows past
 * min(store ceiling, per-row keep.age). The caller advances the offset by
 * (scanned - deleted) and stops when a page comes back short.
 */
export const sweepState = Redis.script(
  (prefix: string, state: string, ttlMs: string, limit: number, offset: number, now: number) => [
    prefix,
    state,
    ttlMs,
    limit,
    offset,
    now
  ],
  {
    numberOfKeys: 0,
    lua: `${HELPERS}
local state = ARGV[2]
local ttl = ARGV[3] ~= "" and tonumber(ARGV[3]) or nil
local limit = tonumber(ARGV[4])
local offset = tonumber(ARGV[5])
local now = tonumber(ARGV[6])
local batch = redis.call("ZRANGEBYSCORE", prefix .. ":finished:" .. state, "-inf", now,
  "WITHSCORES", "LIMIT", offset, limit)
local scanned = 0
local deleted = 0
for i = 1, #batch, 2 do
  scanned = scanned + 1
  local id = batch[i]
  local finishedAt = tonumber(batch[i + 1])
  if redis.call("EXISTS", jobKey(id)) == 0 then
    -- Orphaned member (hash evicted/removed out of band): self-heal so the
    -- cursor math stays honest and the member never loops the sweep.
    redis.call("ZREM", prefix .. ":finished:" .. state, id)
    deleted = deleted + 1
  else
    local cutoffAge = ttl
    local keepJson = redis.call("HGET", jobKey(id), "keep")
    if keepJson and keepJson ~= "" then
      local ok, keep = pcall(cjson.decode, keepJson)
      if ok and type(keep) == "table" then
        local policy = keep[state]
        if type(policy) ~= "table"
          and keep.completed == nil and keep.failed == nil and keep.cancelled == nil
          and (keep.count ~= nil or keep.ageMs ~= nil)
        then
          policy = keep
        end
        if type(policy) == "table" and policy.ageMs ~= nil then
          local age = tonumber(policy.ageMs)
          if age ~= nil and (cutoffAge == nil or age < cutoffAge) then cutoffAge = age end
        end
      end
    end
    if cutoffAge ~= nil and finishedAt <= now - cutoffAge then
      deleteJob(id)
      deleted = deleted + 1
    end
  end
end
return cjson.encode({ scanned = scanned, deleted = deleted })
`
  }
).withReturnType<string>()

/**
 * sweepDedupes(prefix, limit, now) -> number pruned (bounded batch; the
 * caller loops until 0): expired windows, then pending pointers (+inf)
 * whose job is gone or terminal.
 */
export const sweepDedupes = Redis.script(
  (prefix: string, limit: number, now: number) => [prefix, limit, now],
  {
    numberOfKeys: 0,
    lua: `${HELPERS}
local limit = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
-- Lazy migration: drain the pre-0.3 unsplit finished zset into the per-state
-- keys (or drop orphans) so old history keeps getting swept.
local migrated = 0
local legacy = redis.call("ZRANGE", prefix .. ":finished", 0, limit - 1, "WITHSCORES")
for i = 1, #legacy, 2 do
  local id = legacy[i]
  local state = redis.call("HGET", jobKey(id), "state")
  if state == "completed" or state == "failed" or state == "cancelled" then
    redis.call("ZADD", prefix .. ":finished:" .. state, tonumber(legacy[i + 1]), id)
  end
  redis.call("ZREM", prefix .. ":finished", id)
  migrated = migrated + 1
end
local index = prefix .. ":dedupes"
local expired = redis.call("ZRANGEBYSCORE", index, "-inf", now, "LIMIT", 0, limit)
for _, member in ipairs(expired) do
  redis.call("DEL", prefix .. ":dedupe:" .. member)
  redis.call("ZREM", index, member)
end
local removedPending = 0
local pendings = redis.call("ZRANGEBYSCORE", index, "inf", "inf", "LIMIT", 0, limit)
for _, member in ipairs(pendings) do
  local sk = prefix .. ":dedupe:" .. member
  local jobId = redis.call("HGET", sk, "jobId")
  local state = jobId and redis.call("HGET", jobKey(jobId), "state")
  if not state or state == "completed" or state == "failed" or state == "cancelled" then
    redis.call("DEL", sk)
    redis.call("ZREM", index, member)
    removedPending = removedPending + 1
  end
end
return tostring(migrated + #expired + removedPending)
`
  }
).withReturnType<string>()
