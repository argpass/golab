package internal

import "github.com/nsqio/nsq/nsqd"

type Pipe struct {
}

func (p *Pipe) Pub(topic nsqd.Topic) {
}

type Beater interface {
	Beats() <-chan Beat
}
