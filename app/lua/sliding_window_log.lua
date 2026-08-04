-- Sliding-window-log rate limiter.
--
-- The whole check-and-increment runs inside this script, which Redis executes
-- as a single atomic unit. That is the point of the design: two concurrent
-- requests cannot both read "under limit" and then both write, because the
-- second one cannot start until the first has finished writing.
--
-- The window is a sorted set of one member per accepted request, scored by the
-- millisecond it arrived. Expiring the window is just dropping the members
-- whose score has aged out, which gives a true rolling window rather than the
-- burst-at-the-boundary behaviour of fixed buckets.
--
-- The allowed/denied counters are bumped in here too, so a request cannot be
-- counted differently from how it was decided.
--
-- KEYS[1] - the rate limit key for this client
-- KEYS[2] - the metrics hash for this client
-- ARGV[1] - window length in milliseconds
-- ARGV[2] - maximum requests allowed within the window
-- ARGV[3] - unique member id for this request
--
-- Returns {allowed, remaining, retry_after_ms}

local key = KEYS[1]
local metrics_key = KEYS[2]
local window_ms = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local member = ARGV[3]

-- Take the clock from Redis, not from the caller. Every app instance talks to
-- this one server, so a single clock keeps the window consistent no matter how
-- far the instances' own clocks have drifted apart.
local time = redis.call('TIME')
local now_ms = (tonumber(time[1]) * 1000) + math.floor(tonumber(time[2]) / 1000)
local cutoff = now_ms - window_ms

-- Drop the requests that have aged out of the window.
redis.call('ZREMRANGEBYSCORE', key, '-inf', cutoff)

local count = redis.call('ZCARD', key)

if count < limit then
    redis.call('ZADD', key, now_ms, member)
    -- Refresh the TTL so idle clients clean themselves up instead of leaking
    -- a key each.
    redis.call('PEXPIRE', key, window_ms)
    redis.call('HINCRBY', metrics_key, 'allowed', 1)
    return {1, limit - count - 1, 0}
end

-- Denied. The caller can retry once the oldest request in the window ages out.
local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
local retry_after_ms = 0
if oldest[2] then
    retry_after_ms = (tonumber(oldest[2]) + window_ms) - now_ms
    if retry_after_ms < 0 then
        retry_after_ms = 0
    end
end

redis.call('HINCRBY', metrics_key, 'denied', 1)
return {0, 0, retry_after_ms}
