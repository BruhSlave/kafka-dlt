package main

import (
	"context"
	"encoding/json"
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

		fetches.EachError(func(topic string, partition int, err error) {
			log.Printf("fetch error topic=%s partition=%d: %v", topic, partition, err)
		})

		for _, record := range fetches.Records() {
			var event Event
			if err := json.Unmarshal(record.Value, &event); err != nil {
				log.Printf("invalid event at offset %d: %v", record.Offset, err)
				continue
			}

			time.Sleep(time.Duration(cfg.ProcessingDelay) * time.Millisecond)

			inserted, err := sink.SaveEvent(
				ctx,
				event,
				cfg.ConsumerID,
				record.Topic,
				record.Partition,
				record.Offset,
			)

			if err != nil {
				log.Printf("save error: %v", err)
				continue
			}

			if !inserted {
				log.Printf("duplicate event_id=%s", event.EventID)
			}

			if err := client.CommitRecords(ctx, record); err != nil {
				log.Printf("commit error: %v", err)
				continue
			}

			log.Printf(
				"processed user=%s seq=%d partition=%d offset=%d",
				event.UserID,
				event.Seq,
				record.Partition,
				record.Offset,
			)
		}
	}
}
