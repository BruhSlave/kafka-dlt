
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func runConsumer(ctx context.Context, client *kgo.Client, sink *Sink, cfg Config) {
	for {
		if ctx.Err() != nil {
			return
		}

		fetches := client.PollFetches(ctx)

		if ctx.Err() != nil {
			return
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			log.Printf("fetch error topic=%s partition=%d: %v", topic, partition, err)
		})

		for _, record := range fetches.Records() {
			if ctx.Err() != nil {
				return
			}

			err := processWithRetry(ctx, sink, cfg, record)

			if err != nil {
				log.Printf(
					"processing failed after retries topic=%s partition=%d offset=%d: %v",
					record.Topic, record.Partition, record.Offset, err,
				)

				if dltErr := sendToDLT(ctx, client, cfg, record, err.Error()); dltErr != nil {
					log.Printf("DLT send failed: %v", dltErr)
					continue
				}

				log.Printf("sent to DLT topic=%s offset=%d", record.Topic, record.Offset)
			}

			if err := client.CommitRecords(ctx, record); err != nil {
				log.Printf("commit error: %v", err)
				continue
			}
		}
	}
}

func processWithRetry(
	ctx context.Context,
	sink *Sink,
	cfg Config,
	record *kgo.Record,
) error {
	var event Event
	if err := json.Unmarshal(record.Value, &event); err != nil {
		return err
	}

	var lastErr error

	for attempt := 1; attempt <= 3; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		time.Sleep(time.Duration(cfg.ProcessingDelay) * time.Millisecond)

		if isPoison(event) {
			lastErr = errors.New("poison event")
		} else {
			inserted, err := sink.SaveEvent(
				ctx,
				event,
				cfg.ConsumerID,
				record.Topic,
				int(record.Partition),
				record.Offset,
			)

			if err == nil {
				if !inserted {
					log.Printf("duplicate event_id=%s", event.EventID)
				}
				log.Printf(
					"processed user=%s seq=%d partition=%d offset=%d attempt=%d",
					event.UserID, event.Seq, record.Partition, record.Offset, attempt,
				)
				return nil
			}

			lastErr = err
		}

		log.Printf(
			"retry %d/3 topic=%s partition=%d offset=%d: %v",
			attempt, record.Topic, record.Partition, record.Offset, lastErr,
		)
	}

	return lastErr
}

func isPoison(event Event) bool {
	var payload struct {
		Poison bool `json:"poison"`
	}

	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return false
	}

	return payload.Poison
}

