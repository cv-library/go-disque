package disque

import (
	"time"

	"github.com/gomodule/redigo/redis"
)

type Pool struct{ redis.Pool }

func New(addr string) *Pool {
	return &Pool{
		redis.Pool{
			Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", addr) },
			IdleTimeout: 240 * time.Second,
			MaxIdle:     3,
		},
	}
}

type AddOptions struct {
	Async     bool          `redis:"-"`
	Delay     time.Duration `redis:"-"`
	MaxLen    uint          `redis:"MAXLEN,omitempty"`
	Replicate uint16        `redis:"REPLICATE,omitempty"`
	TTL       time.Duration `redis:"-"`
}

func (p *Pool) Add(q, job string, t time.Duration, o *AddOptions) (string, error) {
	// Convert TIMEOUT to milliseconds.
	args := redis.Args{q, job, t.Nanoseconds() / 1e6}.AddFlat(o)

	if o != nil {
		if o.Async {
			args = append(args, "ASYNC")
		}

		if o.Delay >= time.Second {
			// Convert DELAY to seconds.
			args = append(args, "DELAY", int64(o.Delay/time.Second))
		}

		if o.TTL >= time.Second {
			// Convert TTL to seconds.
			args = append(args, "TTL", int64(o.TTL/time.Second))
		}
	}

	conn := p.Pool.Get()
	defer conn.Close()

	if reply, err := conn.Do("ADDJOB", args...); err != nil {
		return "", err
	} else {
		return reply.(string), nil
	}
}

func (p *Pool) Ping() (string, error) {
	conn := p.Pool.Get()
	defer conn.Close()

	if reply, err := conn.Do("PING"); err != nil {
		return "", err
	} else {
		return reply.(string), nil
	}
}

// Private method used in testing.
// https://github.com/antirez/disque/issues/14
func (p *Pool) flush() {
	conn := p.Pool.Get()
	defer conn.Close()

	conn.Do("DEBUG", "FLUSHALL")
}
