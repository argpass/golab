package ari

import (
	"testing"
	"time"

	"github.com/argpass/golab/harvester/libs"
)

func TestDumpEntry(t *testing.T) {
	et := libs.NewEntry(
		"gw_type", uint64(time.Now().Unix()),
		"[I 23234123412] hello from mock data")
	et.AddStringTag("mock")
	et.AddStringField("sn", "sn-xxxx")
	data := DumpEntry(et)
	t.Logf("data:%s\n", data)
}
