package fb

import (
	"net"
	"context"
)

type ProtocolLooper interface {
	IOLoop(context.Context, net.Conn) error
}

