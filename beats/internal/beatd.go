package internal

import (
	"context"
	"sync"
)

type subEvent struct {
	topicID    string
	subscriber Subscriber
}

type pubEvent struct {
	topicID   string
	publisher IsPublisher
}

type beatsDeamon struct {
	wg        sync.WaitGroup
	mu        sync.RWMutex
	topics    map[string]*Topic
	subEventC chan subEvent
	pubEventC chan pubEvent
}

func (b *beatsDeamon) Wait() {
	b.wg.Wait()
}

func (b *beatsDeamon) unsafeEnsureTopic(ctx context.Context, topicID string) *Topic {
	topic := NewTopic(topicID)
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		topic.Pumping(ctx)
	}()
	return topic
}

func (b *beatsDeamon) Sub(ctx context.Context, topicID string, subscriber Subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()
	topic := b.unsafeEnsureTopic(ctx, topicID)
	select {
	case <-ctx.Done():
		return
	case topic.SubEventChan() <- subscriber:
	}
}

func (b *beatsDeamon) Pub(ctx context.Context, topicID string, publisher IsPublisher) {
	b.mu.Lock()
	defer b.mu.Unlock()
	topic := b.unsafeEnsureTopic(ctx, topicID)
	select {
	case topic.PubEventChan() <- publisher:
	case <-ctx.Done():
		return
	}
}
