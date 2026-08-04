-- Token bucket rate limiter.
--
-- Each client holds a bucket that refills continuously at `limit` tokens per
-- `window_ms`. A request costs one token; if the bucket is empty it is denied.
--
-- Unlike the sliding window log this keeps no per-request history, only two
-- numbers: how many tokens are left and when they were last counted. Refill is
-- computed from elapsed time on read rather than by a background job, so the
-- state stays O(1) per client no matter how large the limit is.
--
-- Like the log version, the whole read-modify-write happens inside this script,
-- so concurrent requests cannot both refill from the same stale token count.
--
-- KEYS[1] - the bucket state hash for this client
-- KEYS[2] - the metrics hash for this client
-- ARGV[1] - window length in milliseconds
-- ARGV[2] - bucket capacity, also the tokens refilled per window
--
-- Returns {allowed, remaining, retry_after_ms}

local key = KEYS[1]
local metrics_key = KEYS[2]
local window_ms = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])

-- Take the clock from Redis, so every instance measures refill against the
-- same timeline rather than its own.
local time = redis.call('TIME')
local now_ms = (tonumber(time[1]) * 1000) + math.floor(tonumber(time[2]) / 1000)

-- Tokens per millisecond.
local rate = capacity / window_ms

local state = redis.call('HMGET', key, 'tokens', 'updated_ms')
local tokens = tonumber(state[1])
local updated_ms = tonumber(state[2])

if tokens == nil or updated_ms == nil then
    -- An unseen client starts with a full bucket.
    tokens = capacity
    updated_ms = now_ms
end

-- Refill for the time that has passed, never above capacity. Holding the
-- surplus would let an idle client bank an unbounded burst.
local elapsed = now_ms - updated_ms
if elapsed > 0 then
    tokens = math.min(capacity, tokens + (elapsed * rate))
end

local allowed = 0
local retry_after_ms = 0

if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
    redis.call('HINCRBY', metrics_key, 'allowed', 1)
else
    -- How long until a whole token exists.
    retry_after_ms = math.ceil((1 - tokens) / rate)
    redis.call('HINCRBY', metrics_key, 'denied', 1)
end

redis.call('HSET', key, 'tokens', tokens, 'updated_ms', now_ms)

-- Once a full window has passed with no traffic the bucket has refilled to
-- capacity, which is what a brand new client gets anyway, so the key can go.
redis.call('PEXPIRE', key, math.ceil(window_ms) + 1000)

return {allowed, math.floor(tokens), retry_after_ms}
