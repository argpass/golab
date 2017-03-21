package ari

import (
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
