package ari

import (
	"bytes"
	"testing"
)

func TestMakeRowKey(t *testing.T) {
	var offset uint64 = 11 * 1024 * 1024 * 1024
	var fd uint16 = 259
	key := MakeChunkID(fd, offset)
	if key.FD() != fd {
		t.Logf("fd not equal, got:%d\n", key.FD())
		t.Fail()
	}
	if key.Offset() != offset {
		t.Logf("offset not equal, got:%d\n", key.Offset())
		t.Fail()
	}
}

func TestChunkBuilder_Build(t *testing.T) {
	row := []byte("hello world")
	ckb := NewChunkBuilder()
	var rows = map[uint16][]byte{}
	for i := 0; i < 65535; i++ {
		ckb.AddRow(row)
		rows[uint16(i)] = nil
	}
	ck := ckb.Build()
	data := ck.Bytes()
	bf := bytes.NewBuffer(data)
	ck, err := ReadChunk(bf)
	if err != nil {
		panic(err)
	}
	err = ck.ResolveRows(rows)
	if err != nil {
		panic(err)
	}
	t.Logf("is compressed?:%v\n", ck.Compressed)
	if err != nil {
		panic(err)
	}
	for _, r := range rows {
		if string(r) != string(row) {
			t.Logf("expect %s, but get :%s", row, string(r))
			t.FailNow()
		}
	}
}
