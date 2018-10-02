package disque

import (
	"time"

	"github.com/gomodule/redigo/redis"
)

type AddOptions struct {
	Async     bool          `redis:"-"`
	Delay     time.Duration `redis:"-"`
	MaxLen    uint          `redis:"MAXLEN,omitempty"`
	Replicate uint16        `redis:"REPLICATE,omitempty"`
	TTL       time.Duration `redis:"-"`
}

type GetOptions struct {
	Count        uint          `redis:"COUNT,omitempty"`
	NoHang       bool          `redis:"-"`
	Timeout      time.Duration `redis:"-"`
	WithCounters bool          `redis:"-"`
}

type Job struct {
	Queue, ID, Body             string
	Nacks, AdditionalDeliveries int64
}

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

func (p *Pool) Ack(job Job) error {
	conn := p.Pool.Get()
	defer conn.Close()

	_, err := conn.Do("ACKJOB", job.ID)
	return err
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

	return redis.String(conn.Do("ADDJOB", args...))
}

func (p *Pool) Get(o *GetOptions, q ...string) ([]Job, error) {
	args := redis.Args{}.AddFlat(o)

	if o != nil {
		if o.NoHang {
			args = append(args, "NOHANG")
		}

		if o.Timeout >= time.Millisecond {
			args = append(args, "TIMEOUT", int64(o.Timeout/time.Millisecond))
		}

		if o.WithCounters {
			args = append(args, "WITHCOUNTERS")
		}
	}

	args = args.Add("FROM").AddFlat(q)

	conn := p.Pool.Get()
	defer conn.Close()

	var jobs []Job

	reply, err := conn.Do("GETJOB", args...)

	if rows, ok := reply.([]interface{}); ok {
		for _, row := range rows {
			data := row.([]interface{})

			job := Job{
				Queue: string(data[0].([]byte)),
				ID:    string(data[1].([]byte)),
				Body:  string(data[2].([]byte)),
			}

			if o != nil && o.WithCounters {
				job.Nacks = data[4].(int64)
				job.AdditionalDeliveries = data[6].(int64)
			}

			jobs = append(jobs, job)
		}
	}

	return jobs, err
}

func (p *Pool) Len(queue string) (int, error) {
	conn := p.Pool.Get()
	defer conn.Close()

	return redis.Int(conn.Do("QLEN", queue))
}

func (p *Pool) Nack(job Job) error {
	conn := p.Pool.Get()
	defer conn.Close()

	_, err := conn.Do("NACK", job.ID)
	return err
}

func (p *Pool) Ping() (string, error) {
	conn := p.Pool.Get()
	defer conn.Close()

	return redis.String(conn.Do("PING"))
}

// Private method used in testing.
// https://github.com/antirez/disque/issues/14
func (p *Pool) flush() {
	conn := p.Pool.Get()
	defer conn.Close()

	conn.Send("DEBUG", "FLUSHALL")
}
