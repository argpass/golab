package ari

import (
	"github.com/elastic/beats/libbeat/outputs/transport"
	"time"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/outputs/mode"
	"github.com/elastic/beats/libbeat/common"
	"github.com/dbjtech/golab/harvester/shipper/fb/protocol/v1"
	"github.com/golang/protobuf/proto"
	"fmt"
	"github.com/pkg/errors"
)

var _ mode.ProtocolClient = &client{}

type client struct {
	*transport.Client
}

func newClient(tc *transport.Client) *client {
	return &client{
		Client:tc,
	}
}

func (c *client) Close() error {
	//return c.Client.Close()
	return nil
}

// PublishEvent calls PublishEvents
func (c *client) PublishEvent(data outputs.Data) error {
	_, err := c.PublishEvents([]outputs.Data{data,})
	if err != nil {
		return err
	}
	return nil
}

// PublishEvents publishes events to server by a bulk message
func (c *client) PublishEvents(data []outputs.Data) (nextEvents []outputs.Data, err error) {
	defer func(){
		// todo-remove: debug info by logger instead of `fmt`
		fmt.Printf("pub evts with err:%+v, data count:%d\n", err, len(data))
	}()
	
	buf, err := serializeEvents(data)
	if err != nil {
		return data, errors.Wrap(err, "serialize events")
	}
	bulk, err := makeMessageBytes(buf)
	if err != nil {
		return data, errors.Wrap(err, "make message bytes")
	}
	// send bulk data to the server
	_, err = c.Write(bulk)
	if err != nil {
		return data, errors.Wrap(err, "write bulk")
	}
	return nil, nil
}

func (c *client) Connect(timeout time.Duration) error {
	if !c.Client.IsConnected() {
		fmt.Println("connect")
		err := c.Client.Connect()
		if err != nil {
			fmt.Printf("try to connect ari server get err:%v\n", err)
			return err
		}
	}
	return nil
}

// serializeEvents only picks "type","@timestamp","hostname" and "message" out
func serializeEvents(datas []outputs.Data) ([]byte, error) {
	var bulk v1.Bulk
	for _, data := range datas {
		row := v1.LogRow{}
		d, e := data.Event.GetValue("type")
		if e != nil {
			return nil, e
		}
		row.LogType = d.(string)
		d, e = data.Event.GetValue("@timestamp")
		if e != nil {
			return nil, e
		}
		t := d.(common.Time)
		row.Timestamp = time.Time(t).UnixNano()
		d, e = data.Event.GetValue("beat.hostname")
		if e != nil {
			return nil, e
		}
		row.HostName = d.(string)
		d, e = data.Event.GetValue("message")
		if e != nil {
			return nil, e
		}
		row.Body = d.(string)
		bulk.Rows = append(bulk.Rows, &row)
	}
	buf, err := proto.Marshal(&bulk)
	if err != nil {
		return buf, errors.Wrap(err, "marshal events")
	}
	return buf, err
}

func makeMessageBytes(buf []byte) ([]byte, error) {
	var msg v1.Message
	msg.MsgID = uint64(time.Now().UnixNano())
	msg.Size = uint32(len(buf))
	msg.Data = buf
	return msg.Pack()
}
