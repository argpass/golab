package fb

import (
	"net"
	"fmt"
	"runtime"
	"context"
	"time"
	"go.uber.org/zap"
	"github.com/dbjtech/golab/harvester/libs/constant"
	"github.com/dbjtech/golab/harvester/shipper/fb/protocol/v1"
	"github.com/dbjtech/golab/harvester/harvesterd"
)

type TCPHandler interface {
	Handle(context.Context, net.Conn)
}

func RunTCPServer(ctx context.Context, listener *net.TCPListener, handler TCPHandler)  {
	logger := ctx.Value(constant.KEY_LOGGER).(*zap.Logger)
	logger.Warn(fmt.Sprintf("listen on:%s", listener.Addr()))
	for {
		select {
		case <-ctx.Done():
			break
		default:
		}
		con, err := listener.Accept()
		if err != nil {
			if nerr, ok := err.(net.Error); ok {
				if nerr.Temporary() {
					// temporary err, next
					logger.Warn("temporary Accept() err")
					runtime.Gosched()
					continue
				}
				if nerr.Timeout() {
					// timeout, ignore
					listener.SetDeadline(time.Now().Add(3 * time.Second))
					continue
				}
			}
			// errors that can't be ignored, exit
			logger.Error(fmt.Sprintf("Accept() error:%s", err))
			break
		}
		// start a routine to handle the connection
		go handler.Handle(ctx, con)
	}
	// i'm dead
	logger.Warn(fmt.Sprintf("closing %s", listener.Addr()))
}

// tcpServer is a simple `TCPHandler` implement
type tcpServer struct {
	sendC chan <- []*harvesterd.Entry
}

func (server *tcpServer) Handle(ctx context.Context, con net.Conn) {
	logger := ctx.Value(constant.KEY_LOGGER).(*zap.Logger)
	logger.Info("new connection", zap.String("addr", con.RemoteAddr().String()))
	
	// todo: create looper by client version
	// read client version
	// client should send 4 bytes to report its version
	//buf := make([]byte, 4)
	//_, err := io.ReadFull(con, buf)
	//if err != nil {
	//	logger.Error(fmt.Sprintf("read client protocol error:%s", err))
	//	return
	//}
	
	connctionId := uint64(time.Now().UnixNano())
	looper := v1.NewLooperV1(connctionId)
	// start iolooping
	// todo: record active connection count
	err := looper.IOLoop(ctx, con, server.sendC)
	if err != nil {
		logger.Error(fmt.Sprintf("ioloop err:%s", err))
		return
	}
}

