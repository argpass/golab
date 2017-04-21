package ari

import (
	"testing"
	"github.com/colinmarc/hdfs"
	"time"
)

func TestHdfsAppender_Append(t *testing.T) {
	t.Log("start...")
	c, err := hdfs.New("localhost:9000")
	if err != nil {
		t.Logf("err to new client:%v", err)
		t.Fail()
	}
	hdfsport := &HdfsPort{Client:c}
	defer hdfsport.Close()
	appender, err := NewHdfsAppender(
		"test", "db_t", "123", 0, hdfsport, func(fd uint16) error {
		t.Log("free fd")
		return nil
	})
	if err != nil {
		t.Logf("err:%v", err)
		t.Fail()
	}
	t_start := time.Now().Unix()
	// 1gb
	size := 1024 * 1024 * 1024 * 1
	cur := 0
	for {
		data := []byte("hello world")
		_, err := appender.Append(data)
		if err != nil {
			t.Logf("err on appending:%v", err)
			t.Fail()
		}
		cur += len(data)
		if cur >= size {
			break
		}
	}
	t_end := time.Now().Unix()
	t.Logf("write data %d done cost:%d", cur, t_end - t_start)
	err = appender.Close()
	if err != nil {
		t.Logf("err on closing:%v", err)
		t.Fail()
	}
}
