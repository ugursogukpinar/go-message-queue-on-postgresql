package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/lib/pq"
)

var (
	db      *sql.DB
	connStr *string
)

type workerFn func(data *Task)

// Task represents a task in the queue table
type Task struct {
	ID        int       `json:"id"`
	TaskName  string    `json:"task_name"`
	Payload   JSONB     `json:"payload"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// JSONB is a custom type for handling JSONB data from PostgreSQL
type JSONB map[string]interface{}

// Scan implements the sql.Scanner interface for JSONB
func (j *JSONB) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("type assertion to []byte failed")
	}
	return json.Unmarshal(bytes, j)
}

// Value implements the driver.Valuer interface for JSONB
func (j JSONB) Value() (driver.Value, error) {
	return json.Marshal(j)
}

func get_task(db *sql.DB, task_id int) (*Task, error) {
	// Get a Tx for making transaction requests.
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	// Defer a rollback in case anything fails.
	defer tx.Rollback()

	query := `
      SELECT 
        id, task_name, payload, status, created_at, updated_at FROM tasks WHERE id=$1
      ORDER BY created_at ASC
      LIMIT 1 FOR UPDATE SKIP LOCKED;
  `

	var (
		id         int
		task_name  string
		payload    string
		status     string
		created_at time.Time
		updated_at time.Time
	)

	err = tx.QueryRowContext(ctx, query, task_id).Scan(&id, &task_name, &payload, &status, &created_at, &updated_at)

	if err == sql.ErrNoRows {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	jsonbPayload := JSONB{}
	jsonbPayload.Scan(payload)

	task := &Task{
		ID:        id,
		TaskName:  task_name,
		Payload:   jsonbPayload,
		Status:    status,
		CreatedAt: created_at,
		UpdatedAt: updated_at,
	}

	tx.ExecContext(ctx, "UPDATE tasks SET status = 'processing' WHERE id = $1", task_id)

	if err = tx.Commit(); err != nil {
		return nil, err
	}

	return task, nil
}

func run_worker(listenChannel string, fn workerFn) {
	// Open a new listener connection
	listener := pq.NewListener(*connStr, 10*time.Second, time.Minute, func(ev pq.ListenerEventType, err error) {
		if err != nil {
			log.Fatalf("Listener error: %v", err)
		}
	})

	err := listener.Listen(listenChannel)
	if err != nil {
		log.Fatalf("Error listening on channel %s: %v", listenChannel, err)
	}

	fmt.Printf("Listening on channel %s...\n", listenChannel)

	for {
		select {
		case n := <-listener.Notify:
			if n != nil {
				go func() {
					task_id, _ := strconv.Atoi(n.Extra)

					fmt.Println("task_id", n.Extra)
					task, err := get_task(db, task_id)
					if err != nil {
						log.Printf("Error occured on processing task. task_id=%d, err=%s", task_id, err)
						return
					}

					if task == nil {
						return
					}

					fn(task)
				}()
			}
		case <-time.After(90 * time.Second):
			// Reconnect if no notification received in 90 seconds
			go func() {
				err := listener.Ping()
				if err != nil {
					log.Fatalf("Ping error: %v", err)
				}
			}()
		}
	}
}

const LISTEN_CHANNEL = "pg_tasks_channel"

func main() {
	connStr = flag.String("conn", "", "PostgreSQL connection string")
	workerCount := flag.Int("count", 1, "Worker count")
	flag.Parse()

	var err error
	db, err = sql.Open("postgres", *connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create a signal channel to handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	for i := 0; i < *workerCount; i++ {
		go func(worker_id int) {
			run_worker(LISTEN_CHANNEL, func(task *Task) {
				fmt.Printf("[WORKER=%d] Processing task: %d\n", worker_id, task.ID)

				time.Sleep(1000)
				db.QueryRowContext(context.Background(), "UPDATE tasks SET status = 'done' WHERE id=$1", task.ID)
				fmt.Printf("[WORKER=%d] Task complete: %d\n", worker_id, task.ID)
			})
		}(i + 1)
	}

	<-sigCh
	fmt.Println("Shutting down listeners...")
}
