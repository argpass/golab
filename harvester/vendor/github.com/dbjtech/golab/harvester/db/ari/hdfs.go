package ari

import (
	"github.com/colinmarc/hdfs"
	"fmt"
	"sync"
	"github.com/dbjtech/golab/harvester/harvesterd"
	"os"
	"strings"
	"github.com/dbjtech/golab/harvester/libs"
	"github.com/pkg/errors"
)


// HdfsPort
type HdfsPort struct {
	*hdfs.Client
}

func (h *HdfsPort) GetFileName(dirpath string, fd uint16) string {
	return fmt.Sprintf("%s/%d", dirpath, fd)
}

// GetDirPath "/ari/{cluster}/{db}/{shard}
func (h *HdfsPort) GetDirPath(cluster string, db string, shard string) string  {
	dir := fmt.Sprintf("/ari/%s/%s/%s",cluster, db, shard)
	return dir
}

// ResolveRows resolves content of keys
func (h *HdfsPort) ResolveRows(dirpath string, keysMap map[RowKey][]byte) error {
	// chunkId => serialsMap
	// serialsMap: map[serial]data
	chunkPlan := map[ChunkID]map[uint16][]byte{}
	for rowKey := range keysMap {
		chunkId := rowKey.ChunkID

		if _, ok := chunkPlan[chunkId]; !ok {
			chunkPlan[chunkId] = map[uint16][]byte{}
		}
		chunkPlan[chunkId][rowKey.Serial] = nil
	}
	for chunkId, serialsMap := range chunkPlan {
		pos := fmt.Sprintf("dirpath:%s, fd:%d, off:%d",
			dirpath, chunkId.FD(), chunkId.Offset())
		
		// fetch chunk
		fi, err := h.Client.Open(h.GetFileName(dirpath, chunkId.FD()))
		if err != nil {
			return errors.Wrapf(err, "open hdfs file, %s", pos)
		}
		_, err = fi.Seek(int64(chunkId.Offset()), 0)
		if err != nil {
			return errors.Wrapf(err, "chunk offset, %s", pos)
		}
		chunk, err := ReadChunk(fi)
		if err != nil {
			return errors.Wrapf(err, "read chunk %s", pos)
		}
		// no such chunk
		if chunk == nil {
			return errors.New(fmt.Sprintf("no such chunk %s", pos))
		}
		
		// resolve in the chunk
		err = chunk.ResolveRows(serialsMap)
		if err != nil {
			return errors.Wrapf(err, "resolve rows %s", pos)
		}
		// put data to keysMap
		for serial, data := range serialsMap {
			keysMap[MakeRowKey(chunkId.FD(), chunkId.Offset(), serial)] = data
		}
	}
	return nil
}

// HdfsAppender is used to append chunks to the `HDFS`.
// the appender holds only one fd to write chunks to file
// `/ari/{cluster}/{db}/{shard}/{fd}`
type HdfsAppender struct {
	lock   sync.RWMutex
	writer *hdfs.FileWriter
	cur    uint64
	closed bool
	fd     uint16
	Db     string
	Shard  string
	freeFdFn func(fd uint16) error
}

func NewHdfsAppender(
	cluster string, db string,
	shard string, fd uint16,
	hdfsPort *HdfsPort, freeFdFn func(fd uint16) error) (*HdfsAppender, error) {

	name := fmt.Sprintf("%d", fd)
	dir := hdfsPort.GetDirPath(cluster, db, shard)
	filepath := fmt.Sprintf("%s/%s", dir, name)

read:
    // ensure dir exist
	fs, err := hdfsPort.ReadDir(dir)
	if err != nil {
		if nerr, ok := err.(*os.PathError);ok {
			if strings.HasSuffix(nerr.Error(), "does not exist") {
				err = hdfsPort.MkdirAll(dir, os.ModeDir)
				if err != nil {
					return nil, err
				}
				goto read
			}
		}
		return nil, err
	}
	var exist bool
	for _, f := range fs {
		if f.Name() == name && !f.IsDir() {
			exist = true
			break
		}
	}
	if !exist {
		err = hdfsPort.CreateEmptyFile(fmt.Sprintf("%s/%s", dir, name))
		if err != nil {
			return nil, err
		}
	}
	w, err := hdfsPort.Append(filepath)
	if err != nil {
		return nil, err
	}
	info, err := hdfsPort.Stat(filepath)
	if err != nil {
		w.Close()
		return nil, err
	}
	size := uint64(info.Size())
	appender := &HdfsAppender{writer: w,cur: size,fd: fd, Db: db,Shard: shard, freeFdFn:freeFdFn}
	return appender, nil
}

func (h *HdfsAppender) GetFD() uint16 {
	return h.fd
}

func (h *HdfsAppender) CurrentOff() (uint64, error) {
	return h.cur, nil
}

// Append chunk data to the hdfs,
// the method is concurrency-safe
func (h *HdfsAppender) Append(data []byte) (offset uint64, err error) {
	h.lock.Lock()
	defer h.lock.Unlock()
	if h.closed {
		err = harvesterd.Error{Code:libs.E_APPENDER_CLOSED}
		return
	}
	_, err = h.writer.Write(data)
	if err != nil {
		return
	}
	offset = h.cur
	h.cur += uint64(len(data))
	return
}

// Close the appender and release fd
func (h *HdfsAppender) Close() error {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.closed = true
	
	// only try to free it, never to raise error on the method
	h.freeFdFn(h.fd)
	
	err := h.writer.Close()
	if err != nil {
		return err
	}
	return nil
}

