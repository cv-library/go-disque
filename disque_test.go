package disque

import (
	"bytes"
	"os"
	"os/exec"
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

	pool = New("127.0.0.1:7711")

	status := m.Run()

	cmd = exec.Command("docker", "kill", strings.TrimSpace(id.String()))

	cmd.Stderr = os.Stderr

	cmd.Run()

	os.Exit(status)
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
		&AddOptions{true, time.Minute, 9, 1, time.Hour},
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

func TestPing(t *testing.T) {
	pool.flush()

	if got, err := pool.Ping(); err != nil {
		t.Fatal(err)
	} else if got != "PONG" {
		t.Errorf("Ping was incorrect, got: %s, want: %s.", got, "PONG")
	}
}
