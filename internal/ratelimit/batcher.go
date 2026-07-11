package ratelimit

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	checkChunkSize = 64
	checkMaxWait   = 100 * time.Microsecond
)

// fixedWindowChunkScript evaluates up to N keys in one Redis round trip.
// KEYS[i] = counter key; ARGV[1] = limit; ARGV[2] = windowMs.
// Returns an array of 1 (allowed) / 0 (blocked) per key, in order.
var fixedWindowChunkScript = redis.NewScript(`
	local limit = tonumber(ARGV[1])
	local window = tonumber(ARGV[2])
	local results = {}
	for i = 1, #KEYS do
		local current = redis.call('INCR', KEYS[i])
		if current == 1 then
			redis.call('PEXPIRE', KEYS[i], window)
		end
		if current > limit then
			results[i] = 0
		else
			results[i] = 1
		end
	end
	return results
`)

type checkRequest struct {
	key      string
	limit    int
	windowMs int
	reply    chan bool
}

// checkBatcher coalesces concurrent Check calls into chunked Redis EVALSHA round trips.
type checkBatcher struct {
	rdb *redis.Client
	ch  chan checkRequest
}

var replyPool = sync.Pool{
	New: func() any {
		return make(chan bool, 1)
	},
}

func newCheckBatcher(rdb *redis.Client) *checkBatcher {
	b := &checkBatcher{
		rdb: rdb,
		ch:  make(chan checkRequest, checkChunkSize*8),
	}
	go b.loop()
	return b
}

func (b *checkBatcher) submit(key string, limit, windowMs int) bool {
	reply := replyPool.Get().(chan bool)
	b.ch <- checkRequest{key: key, limit: limit, windowMs: windowMs, reply: reply}
	ok := <-reply
	// drain in case of double-send, then return to pool
	select {
	case <-reply:
	default:
	}
	replyPool.Put(reply)
	return ok
}

func (b *checkBatcher) loop() {
	buf := make([]checkRequest, 0, checkChunkSize)
	timer := time.NewTimer(checkMaxWait)
	if !timer.Stop() {
		<-timer.C
	}

	flush := func() {
		if len(buf) == 0 {
			return
		}
		b.flush(buf)
		buf = buf[:0]
	}

	for {
		req := <-b.ch
		buf = append(buf, req)

		// Opportunistically drain ready requests up to chunk size.
	drain:
		for len(buf) < checkChunkSize {
			select {
			case req := <-b.ch:
				buf = append(buf, req)
			default:
				break drain
			}
		}

		if len(buf) >= checkChunkSize {
			flush()
			continue
		}

		timer.Reset(checkMaxWait)
	wait:
		for len(buf) < checkChunkSize {
			select {
			case req := <-b.ch:
				buf = append(buf, req)
			case <-timer.C:
				break wait
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		flush()
	}
}

func (b *checkBatcher) flush(batch []checkRequest) {
	// Group by (limit, windowMs) so each Lua call uses a single shared config.
	type cfgKey struct {
		limit    int
		windowMs int
	}
	groups := make(map[cfgKey][]int, 1)
	for i, req := range batch {
		k := cfgKey{limit: req.limit, windowMs: req.windowMs}
		groups[k] = append(groups[k], i)
	}

	ctx := context.Background()
	results := make([]bool, len(batch))

	for cfg, idxs := range groups {
		keys := make([]string, len(idxs))
		for j, i := range idxs {
			keys[j] = batch[i].key
		}
		raw, err := fixedWindowChunkScript.Run(ctx, b.rdb, keys, cfg.limit, cfg.windowMs).Slice()
		if err != nil {
			for _, i := range idxs {
				results[i] = false
			}
			continue
		}
		for j, i := range idxs {
			if j < len(raw) {
				switch v := raw[j].(type) {
				case int64:
					results[i] = v == 1
				default:
					results[i] = false
				}
			}
		}
	}

	for i, req := range batch {
		req.reply <- results[i]
	}
}
