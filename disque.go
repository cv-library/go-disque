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
	Retry     time.Duration `redis:"-"`
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

func New(addr string, idleTimeout time.Duration) *Pool {
	return &Pool{
		redis.Pool{
			Dial:         func() (redis.Conn, error) { return redis.Dial("tcp", addr) },
			IdleTimeout:  idleTimeout,
			MaxIdle:      3,
			TestOnBorrow: func(c redis.Conn, _ time.Time) error { return c.Send("PING") },
		},
	}
}

func (p *Pool) Ack(jobs ...Job) error {
	ids := make([]interface{}, len(jobs))
	for i, job := range jobs {
		ids[i] = job.ID
	}

	conn := p.Pool.Get()
	defer conn.Close()

	return conn.Send("ACKJOB", ids...)
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

		if o.Retry >= time.Second {
			// Convert Retry to seconds.
			args = append(args, "RETRY", int64(o.Retry/time.Second))
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

func (p *Pool) FastAck(jobs ...Job) error {
	ids := make([]interface{}, len(jobs))
	for i, job := range jobs {
		ids[i] = job.ID
	}

	conn := p.Pool.Get()
	defer conn.Close()

	return conn.Send("FASTACK", ids...)
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

func (p *Pool) Nack(jobs ...Job) error {
	ids := make([]interface{}, len(jobs))
	for i, job := range jobs {
		ids[i] = job.ID
	}

	conn := p.Pool.Get()
	defer conn.Close()

	return conn.Send("NACK", ids...)
}

func (p *Pool) Ping() (string, error) {
	conn := p.Pool.Get()
	defer conn.Close()

	return redis.String(conn.Do("PING"))
}

func (p *Pool) Stat(queue string) (map[string]interface{}, error) {
	conn := p.Pool.Get()
	defer conn.Close()

	reply, err := conn.Do("QSTAT", queue)
	if err != nil {
		return nil, err
	}

	values, ok := reply.([]interface{})
	if !ok {
		return nil, nil
	}

	stat := make(map[string]interface{}, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		key := string(values[i].([]byte))

		switch value := values[i+1]; value.(type) {
		case []byte:
			stat[key] = string(value.([]byte))
		case []interface{}:
			values := value.([]interface{})
			newValues := make([]string, len(values))
			for i, v := range values {
				newValues[i] = string(v.([]byte))
			}
			stat[key] = newValues
		case int64:
			stat[key] = value.(int64)
		}
	}

	return stat, err
}

func (p *Pool) Working(job Job) (time.Duration, error) {
	conn := p.Pool.Get()
	defer conn.Close()

	seconds, err := redis.Int(conn.Do("WORKING", job.ID))

	return time.Second * time.Duration(seconds), err
}

// Private method used in testing.
// https://github.com/antirez/disque/issues/14
func (p *Pool) flush() {
	conn := p.Pool.Get()
	defer conn.Close()

	conn.Send("DEBUG", "FLUSHALL")
}
