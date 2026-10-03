package dispatch

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rabbitmq/amqp091-go"
)

const queueName = "tasks.logical"
const deadExchange = "tasks.dead"

func RunOutboxPublisher(ctx context.Context, db *pgxpool.Pool, brokerURL string) {
	for ctx.Err() == nil {
		conn, err := amqp091.Dial(brokerURL)
		if err != nil {
			log.Printf("rabbitmq unavailable: %v", err)
			if !pause(ctx, 2*time.Second) {
				return
			}
			continue
		}
		channel, err := conn.Channel()
		if err == nil {
			err = channel.ExchangeDeclare(deadExchange, "fanout", true, false, false, false, nil)
		}
		if err == nil {
			_, err = channel.QueueDeclare("tasks.dead", true, false, false, false, nil)
		}
		if err == nil {
			err = channel.QueueBind("tasks.dead", "", deadExchange, false, nil)
		}
		if err == nil {
			_, err = channel.QueueDeclare("tasks.logical.retry", true, false, false, false, amqp091.Table{
				"x-message-ttl":             int32(2000),
				"x-dead-letter-exchange":    "",
				"x-dead-letter-routing-key": queueName,
			})
		}
		if err == nil {
			_, err = channel.QueueDeclare(queueName, true, false, false, false, amqp091.Table{
				"x-queue-type":              "quorum",
				"x-dead-letter-exchange":    deadExchange,
				"x-dead-letter-routing-key": "",
			})
		}
		if err == nil {
			err = channel.Confirm(false)
		}
		if err != nil {
			log.Printf("rabbitmq setup failed: %v", err)
			if channel != nil {
				_ = channel.Close()
			}
			_ = conn.Close()
			if !pause(ctx, 2*time.Second) {
				return
			}
			continue
		}
		confirmations := channel.NotifyPublish(make(chan amqp091.Confirmation, 1))
		log.Printf("outbox publisher connected")
		for ctx.Err() == nil {
			published, err := publishOne(ctx, db, channel, confirmations)
			if err != nil {
				log.Printf("outbox publish failed: %v", err)
				break
			}
			if !published && !pause(ctx, 50*time.Millisecond) {
				break
			}
		}
		_ = channel.Close()
		_ = conn.Close()
	}
}

func publishOne(ctx context.Context, db *pgxpool.Pool, channel *amqp091.Channel, confirmations <-chan amqp091.Confirmation) (bool, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var eventID int64
	var taskID string
	var payload []byte
	err = tx.QueryRow(ctx, `SELECT id, task_id::text, payload FROM outbox_events WHERE published_at IS NULL ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED`).Scan(&eventID, &taskID, &payload)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	err = channel.PublishWithContext(ctx, "", queueName, false, false, amqp091.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp091.Persistent,
		MessageId:    taskID,
		Body:         payload,
	})
	if err != nil {
		return false, err
	}
	select {
	case confirmation, open := <-confirmations:
		if !open {
			return false, amqp091.ErrClosed
		}
		if !confirmation.Ack {
			return false, amqp091.ErrClosed
		}
	case <-ctx.Done():
		return false, ctx.Err()
	}
	if _, err := tx.Exec(ctx, `UPDATE outbox_events SET published_at = now() WHERE id = $1`, eventID); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func pause(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
