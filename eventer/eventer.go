package eventer

import (
	"sync"
	"github.com/dbjtech/golab/eventer/port"
	"github.com/dbjtech/golab/eventer/log"
	"fmt"
	"sync/atomic"
	"time"
	"errors"
	"github.com/dbjtech/golab/eventer/handler"
)

const (
	S_INITIAL uint32 = iota
	S_RUNNING
	S_STOPPED
)

// todo: record running status
// todo: (packet count, response count, flying num, ignore bad packet count, running time)
type Eventer struct {
	sync.WaitGroup

	flyingNum     uint32
	flyingMaximum uint32

	status        int32
	doneC         chan struct{}

	gateway       *port.GatewayPort
	logger        log.IsLogger
}

// Stop the eventer, wait for all tasks to exit
func (e *Eventer) Stop()  {
	// already stopped
	status := atomic.LoadUint32(&e.status)
	if status == S_STOPPED {
		return
	}
	// CAS
	if atomic.CompareAndSwapInt32(&e.status, status, 1) {
		// wait all tasks done
		e.logger.Warn("eventer stopping")
		close(e.doneC)
		e.WaitGroup.Wait()
		e.logger.Warn("eventer bye")
	}
}

// Start the eventer
func (e *Eventer) Start() error {
	// change status(should be `S_INITIAL`) to running
	if atomic.CompareAndSwapUint32(&e.status, S_INITIAL, S_RUNNING) {
		// start pumping
		go func(){
			e.WaitGroup.Add(1)
			defer e.WaitGroup.Done()
			e.goPumping()
		}()
		return nil
	}
	// the eventer not be initial status,
	// never to start it again
	return errors.New("status != 0")
}

// GetHandler resolves the handler for the packet
func (e *Eventer) GetHandler(packet *Packet) (handler handler.Handler, found bool) {
	// todo:
	return
}

// goDispatch chooses a handler to handle the packet
func (e *Eventer) goDispatch(packet *Packet) {
	logger := e.logger.With(log.Str("iccid", packet.Datagram.ICCID))
	logger.Debug("dispatch packet", log.Str("datagram_id", packet.DatagramID))
	hdl, ok := e.GetHandler(packet)
	if !ok {
		logger.Error("no handler for the packet")
		return
	}
	err := hdl.Handle()
	if err != nil {
		// todo: handle handler error
		logger.Error(fmt.Sprintf("%+v", err))
	}
}

func (e *Eventer) isStopped() bool {
	return atomic.LoadInt32(&e.status) == S_STOPPED
}

// goPumping pulls packets from the `Gateway RPC`
// and create a goroutine to handle it
func (e *Eventer) goPumping() {
	e.logger.Warn("pumping start")
	pollTimeout := 2
	for {
		if e.isStopped() {
			goto exit
		}
		// sleep 500ms when there are too many flying tasks
		if atomic.LoadUint32(&e.flyingNum) >= e.flyingMaximum {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		// now i can poll packets
		data, err := e.gateway.PollPacket(pollTimeout)
		if err != nil {
			e.logger.Error(fmt.Sprintf("%+v", err), log.Tag("Poll"))
			continue
		}
		// log Recv
		e.logger.Info(string(data), log.Tag("Recv"))
		packet, err := NewPacket(data)
		if err != nil {
			e.logger.Error(fmt.Sprintf("invalid packet:%s, err:%+v", data, err))
		}

		// start a goroutine to handle the packet
		go func() {
			e.WaitGroup.Add(1)
			defer e.WaitGroup.Done()
			// decrement `e.flyingNum` by 1 when the goroutine exits
			defer atomic.AddUint32(&e.flyingNum, ^uint32(0))
			e.goDispatch(packet)
		}()
	}
    exit:
	e.logger.Warn("puping bye")
}

