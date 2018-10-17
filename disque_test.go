package disque

import (
	"bytes"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"
)

var pool *Pool

func TestMain(m *testing.M) {
	cmd := exec.Command("docker", "run", "-dp", "7711:7711", "efrecon/disque:1.0-rc1")

	var id bytes.Buffer
	cmd.Stderr = os.Stderr
	cmd.Stdout = &id

	if err := cmd.Run(); err != nil {
		panic(err)
	}

	pool = New("127.0.0.1:7711", 240*time.Second)

	status := m.Run()

	cmd = exec.Command("docker", "kill", strings.TrimSpace(id.String()))

	cmd.Stderr = os.Stderr

	cmd.Run()

	os.Exit(status)
}

func TestAck(t *testing.T) {
	if err := pool.Ack(Job{ID: "D-49a4ad3b-fCAnZ8eFxSxKyl5fX58KCrGM-05a1"}); err != nil {
		t.Fatal(err)
	}
}

func TestAdd(t *testing.T) {
	pool.flush()

	if _, err := pool.Add("foo", "bar", time.Second, nil); err != nil {
		t.Fatal(err)
	}

	if _, err := pool.Add(
		"foo",
		"bar",
		time.Second,
		&AddOptions{true, time.Second, 9, 1, time.Minute, time.Hour},
	); err != nil {
		t.Fatal(err)
	}
}

func TestAddDelayGreaterThanTTL(t *testing.T) {
	pool.flush()

	exp := "ERR The specified DELAY is greater than TTL. Job refused since would never be delivered"

	if _, err := pool.Add(
		"foo",
		"bar",
		time.Second,
		&AddOptions{Delay: time.Second, TTL: time.Second},
	); err == nil || err.Error() != exp {
		t.Errorf("Error was incorrect, got: %s, want: %s.", err, exp)
	}
}

func TestAddNotEnoughReachableNodes(t *testing.T) {
	pool.flush()

	exp := "NOREPL Not enough reachable nodes for the requested replication level"

	if _, err := pool.Add(
		"foo", "bar", time.Second, &AddOptions{Replicate: 2},
	); err == nil || err.Error() != exp {
		t.Errorf("Error was incorrect, got: %s, want: %s.", err, exp)
	}
}

func TestFastAck(t *testing.T) {
	if err := pool.FastAck(Job{ID: "D-49a4ad3b-fCAnZ8eFxSxKyl5fX58KCrGM-05a1"}); err != nil {
		t.Fatal(err)
	}
}

func TestGet(t *testing.T) {
	pool.flush()

	exp := []Job{{Queue: "foo", Body: "bar"}}

	exp[0].ID, _ = pool.Add("foo", "bar", time.Second, nil)

	if jobs, err := pool.Get(&GetOptions{NoHang: true}, "foo"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(jobs, exp) {
		t.Errorf("Get was incorrect, got: %#v, want: %#v.", jobs, exp)
	}
}

func TestGetMultiWithCounters(t *testing.T) {
	pool.flush()

	exp := []Job{{Queue: "foo", Body: "bar"}, {Queue: "foo", Body: "baz"}}

	exp[0].ID, _ = pool.Add("foo", "bar", time.Second, nil)
	exp[1].ID, _ = pool.Add("foo", "baz", time.Second, nil)

	if jobs, err := pool.Get(&GetOptions{2, true, time.Second, true}, "foo"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(jobs, exp) {
		t.Errorf("Get was incorrect, got: %#v, want: %#v.", jobs, exp)
	}
}

func TestGetNoQueue(t *testing.T) {
	exp := "ERR syntax error"

	if _, err := pool.Get(nil); err == nil || err.Error() != exp {
		t.Errorf("Error was incorrect, got: %s, want: %s.", err, exp)
	}
}

func TestGetEmpty(t *testing.T) {
	pool.flush()

	if _, err := pool.Get(&GetOptions{NoHang: true}, "foo"); err != nil {
		t.Fatal(err)
	}
}

func TestLen(t *testing.T) {
	pool.flush()

	pool.Add("foo", "bar", time.Second, nil)

	if got, err := pool.Len("foo"); err != nil {
		t.Fatal(err)
	} else if got != 1 {
		t.Errorf("Len was incorrect, got: %d, want: %d.", got, 1)
	}
}

func TestNack(t *testing.T) {
	if err := pool.Nack(Job{ID: "D-49a4ad3b-fCAnZ8eFxSxKyl5fX58KCrGM-05a1"}); err != nil {
		t.Fatal(err)
	}
}

func TestPing(t *testing.T) {
	if got, err := pool.Ping(); err != nil {
		t.Fatal(err)
	} else if got != "PONG" {
		t.Errorf("Ping was incorrect, got: %s, want: %s.", got, "PONG")
	}
}

func TestStat(t *testing.T) {
	pool.flush()

	pool.Add("foo", "bar", time.Second, nil)

	exp := map[string]interface{}{
		"age":         int64(0),
		"blocked":     int64(0),
		"idle":        int64(0),
		"import-from": []string{},
		"import-rate": int64(0),
		"jobs-in":     int64(1),
		"jobs-out":    int64(0),
		"len":         int64(1),
		"name":        "foo",
		"pause":       "none",
	}

	if stat, err := pool.Stat("foo"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(stat, exp) {
		t.Errorf("Stat was incorrect, got: %#v, want: %#v.", stat, exp)
	}
}

func TestStatNonExistentQueue(t *testing.T) {
	if stat, err := pool.Stat("non-existent-queue"); err != nil {
		t.Fatal(err)
	} else if stat != nil {
		t.Errorf("Stat was incorrect, got: %#v, want: %#v.", stat, nil)
	}
}

func TestWorking(t *testing.T) {
	pool.flush()

	exp := time.Minute

	job := Job{}
	job.ID, _ = pool.Add("foo", "bar", time.Second, &AddOptions{Retry: exp})

	if got, err := pool.Working(job); err != nil {
		t.Fatal(err)
	} else if got != exp {
		t.Errorf("Working was incorrect, got: %q, want: %q.", got, exp)
	}
}
