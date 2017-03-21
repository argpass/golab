package handler

import (
	"github.com/dbjtech/golab/eventer"
	"github.com/dbjtech/golab/eventer/log"
)

type Handler interface {
	Setup(packet *eventer.Packet,
	      logger log.IsLogger, eventer *eventer.Eventer)
	Handle() error
}

type baseHandler struct {
	eventer *eventer.Eventer
	logger log.IsLogger

	packet *eventer.Packet
}

func (h *baseHandler) Setup(
		packet *eventer.Packet,
	  	logger log.IsLogger,
		eventer *eventer.Eventer) {
	h.logger = logger
	h.packet = packet
	h.eventer = eventer
}

func (h *baseHandler) Handle() error {
	return nil
}
