package v1

import (
	"testing"
	"time"
	"bytes"
	"encoding/binary"
	"io"
	"strings"
)

func TestMessage_Pack(t *testing.T) {
	var msg Message
	buf := []byte("hello world")
	msg.MsgID = uint64(time.Now().UnixNano())
	msg.Size = uint32(len(buf))
	msg.Data = buf
	data, err := msg.Pack()
	if err != nil {
		t.Logf("unexpect err:%v", err)
		t.Fail()
	}
	
	var msgGot Message
	rd := bytes.NewReader(data)
	binary.Read(rd, binary.BigEndian, &msgGot.MessageHeader)
	if msgGot.Size != msg.Size {
		t.Logf("unexpect size, expect:%d, got :%d", msg.Size, msgGot.Size)
		t.Fail()
	}
	msgGot.Data = make([]byte, msgGot.Size)
	_, err = io.ReadFull(rd, msgGot.Data)
	if err != nil {
		t.Logf("unexpect err:%v\n", err)
		t.Fail()
	}
	if !strings.EqualFold(string(msg.Data), string(msgGot.Data)) {
		t.Logf("invalid data")
		t.Fail()
	}
}
