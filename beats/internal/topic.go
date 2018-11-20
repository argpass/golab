package internal

import (
	"context"
)

type Topic struct {
	ID           string
	subEventChan chan Subscriber
	pubEventChan chan IsPublisher
}

func NewTopic(topicID string) *Topic {
	return &Topic{
		ID:           topicID,
		subEventChan: make(chan Subscriber, 1),
		pubEventChan: make(chan IsPublisher, 1),
	}
}

func (t *Topic) SubEventChan() chan<- Subscriber {
	return t.subEventChan
}

func (t *Topic) PubEventChan() chan<- IsPublisher {
	return t.pubEventChan
}

//func (t *Topic) publishing(ctx context.Context, publisher Publisher, subC <-chan Subscriber) {
//	var subscribers []Subscriber
//	for {
//		select {
//		case <-ctx.Done():
//			return
//		case beat := <-publisher.Beats():
//			for _, subscriber := range subscribers {
//				subscriber.Notify(ctx, beat)
//			}
//		case subscriber := <-subC:
//			publisher.Connect(subscriber)
//		}
//	}
//}

func (t *Topic) Pumping(ctx context.Context) {
	var subscribers []Subscriber
	var publishers []IsPublisher
	for {
		select {
		case subscriber := <-t.subEventChan:
			subscribers = append(subscribers, subscriber)
			for _, publisher := range publishers {
				publisher.Connect(subscriber)
			}
		case publisher := <-t.pubEventChan:
			publishers = append(publishers, publisher)
			for _, subscriber := range subscribers {
				publisher.Connect(subscriber)
			}
		}
	}
}
