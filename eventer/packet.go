package eventer

import (
	"time"
	"sync"
	"encoding/json"
)

type Datagram struct {
	MessageType string `json:"message_type"`
	MessageNO  int `json:"message_no"`
	ProductVersion string `json:"product_version"`
	ProtocolVersion int `json:"protocol_version"`
	FirmwareVersion string `json:"firmware_version"`
	ICCID string `json:"iccid"`
	Serial int `json:"serial"`
	SessionID string `json:"session_id"`
	Params map[string]interface{} `json:"params"`
}

/*Packet
{
      "datagrams_id": "127.0.0.1:51107:898600d6101400027602",
      "res_queue_name": "common_gateway:res:00000001",
      "timestamp": 1438160735091,
      "datagrams": {
            "message_type": "U",
            "message_no": 1,
            "product_version": "W",
            "protocol_version": 1,
            "firmware_version": "B",
            "iccid": "898600D6101400027602",
            "serial": 42,
            "session_id": "A1B2C3D4",
            "params": {
            }
      }
}
*/
type Packet struct {
	DatagramID string `json:"datagrams_id"`
	ResQueueName string `json:"res_queue_name"`
	Timestamp int `json:"timestamp"`
	Datagram Datagram `json:"datagrams"`
}

func NewPacket(raw []byte) (*Packet, error) {
	var p = Packet{}
	err := json.Unmarshal(raw, &p)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

/*Resp
{
  "datagrams_id": "127.0.0.1:51107:898600d6101400027602",
  "datagrams": {
    "message_type": "D",
    "message_no": 1,
    "serial": 42,
    "timestamp": 1437997686,
    "status_code": 0,
    "params": {
      "session_id": "A1B2C3D4",
      "login_result": 0
    }
  }
}
 */
type Resp struct {
	DatagramID string
	MessageType string
	MessageNO int
	Serial int
	Timestamp int
	StatusCode int
	Params map[string]interface{}

	RespQueueName string
}

func NewResp(pkt *Packet, status int, result map[string]interface{}) *Resp {
	resp := &Resp{
		DatagramID:pkt.DatagramID,
		MessageType:"D",
		MessageNO:pkt.Datagram.MessageNO,
		Serial:pkt.Datagram.Serial,
		Timestamp: int(time.Now().Unix()),
		StatusCode:status,
		Params:result,
		RespQueueName:pkt.ResQueueName,
	}
	return resp
}

func (resp *Resp)JsonBytes() []byte {
	// todo:
	return nil
}

func (resp *Resp) ResetFor(pkt *Packet, status int, timestamp int,
			result map[string]interface{}) *Resp {
	resp.DatagramID = pkt.DatagramID
	resp.MessageType = pkt.Datagram.MessageType
	resp.MessageNO = pkt.Datagram.MessageNO
	resp.Serial = pkt.Datagram.Serial
	resp.Timestamp = timestamp
	resp.StatusCode = status
	resp.Params = result
	return resp
}

var (
	RespPool sync.Pool
	PacketPool sync.Pool
)

func GetResp() *Resp {
	return RespPool.Get().(*Resp)
}

func GetPacket() *Packet {
	return PacketPool.Get().(*Packet)
}

func ReleaseResp(resp *Resp) {
	RespPool.Put(resp)
}

func ReleasePacket(pkt *Packet) {
	PacketPool.Put(pkt)
}

func init()  {
	// todo: remove the pool
	RespPool.New = func()interface{} {
		return &Resp{}
	}
	// todo: remove the pool
	PacketPool.New = func() interface{} {
		return &Packet{}
	}
}
