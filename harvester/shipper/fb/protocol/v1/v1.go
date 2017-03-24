package v1

import (
	"net"
	"time"
	"io"
	"fmt"
	"encoding/binary"
	"context"
	"runtime"
	"github.com/golang/protobuf/proto"
	"errors"
	"github.com/dbjtech/golab/harvester/libs/constant"
	"go.uber.org/zap"
	"github.com/dbjtech/golab/harvester/libs"
)

// ConvertToEntries converts log rows to entries
// todo: support boolean, float
func ConvertToEntries(rows []*LogRow) []*libs.Entry {
	entries := make([]*libs.Entry, len(rows))
	for i, row := range rows {
		entry := libs.NewEntry(row.LogType, uint64(row.Timestamp), row.GetBody())
		for _, fi := range row.Fields {
			if fi.Type == Field_IsInt {
				entry.AddIntField(fi.Key, fi.Ival)
			}
			if fi.Type == Field_IsString {
				entry.AddStringField(fi.Key, fi.Sval)
			}
		}
		entries[i] = entry
	}
	return entries
}

type MessageHeader struct {
	// Size is the len of `Data`
	Size uint32
	MsgID uint64
}

type Message struct {
	MessageHeader
	Data []byte
}

func (m Message) Pack() ([]byte, error) {
	buf := make([]byte, int(m.Size) + 12)
	binary.BigEndian.PutUint32(buf, m.Size)
	binary.BigEndian.PutUint64(buf, m.MsgID)
	n := copy(buf[12:], m.Data)
	if n != int(m.Size) {
		return nil, errors.New("invalid message size")
	}
	return buf, nil
}

type looperV1 struct {
	id uint64

}

func NewLooperV1(connectionId uint64) *looperV1 {
	return &looperV1{
		id: connectionId,
	}
}

func (l *looperV1) IOLoop(
	ctx context.Context, con net.Conn,
	sendC chan <- []*libs.Entry) error {
	
	var err error
	logger := ctx.Value(constant.KEY_LOGGER).(*zap.Logger)
	logger = logger.With(zap.Int64("con_id", int64(l.id)))
	invalidCounter := 0
	for {
		select {
		case <-ctx.Done():
			goto exit
		default:
		}
		msg := Message{}
		con.SetReadDeadline(time.Now().Add(3 * time.Second))
		err = binary.Read(con, binary.BigEndian, &msg.MessageHeader)
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Timeout(){
				// read timeout, next
				runtime.Gosched()
				continue
			}
			// read err
			goto exit
		}
		
		// todo: never to timeout but how to got exit signal ?
		// never timeout
		con.SetReadDeadline(time.Time{})
		msg.Data = make([]byte, int(msg.Size))
		_, err = io.ReadFull(con, msg.Data)
		if err != nil {
			// read err
			goto exit
		}

		var bulk Bulk
		err := proto.Unmarshal(msg.Data, &bulk)
		if err != nil {
			invalidCounter += 1
			if invalidCounter > 3 {
				err = errors.New(fmt.Sprintf("read %d times invalid protobuf buffer, " +
					"client maybe broken, stop", invalidCounter))
				goto exit
			}
			continue
		}

		entries := ConvertToEntries(bulk.GetRows())

		select {
		case <-ctx.Done():
			goto exit
		case sendC <- entries:
		    // todo: entries accepted,
			// todo: so we should send ACK to the client
		    continue
		}
	}

exit:
	if err != nil {
		logger.Error(fmt.Sprintf("loop done with err:%v", err))
	}else{
		logger.Info("loop done")
	}
	return err
}

