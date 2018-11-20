package rg

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"sync"

	"github.com/pkg/errors"
)

var (
	ErrPointSize       = errors.New("invalid point size")
	ErrMergeLockExists = errors.New("merge file exists")
)

const (
	newKeySep = "'"
)

type Meta struct {
	// PointSize byte num of points
	PointSize int `json:"point_size"`

	// RoundSize data point num of Panels
	RoundSize int `json:"round_size"`

	// Version current version
	Version int `json:"current_version"`
}

type pointT []byte

func newEmptyPoint(size int) pointT {
	return make([]byte, size)
}

func (p pointT) size() int {
	return len(p)
}

type keyPointPair struct {
	key   string
	point pointT
}

type keyIDX struct {
	key     string
	panelNo int
}

type panelT []pointT

type groupT []byte

type mergingInfo struct {
	version uint32
}

// Merging is a transaction process to
// merge current version with new data to the next version
type Merging struct {
	mu            sync.RWMutex
	b             *Bucket
	targetVersion int
	pendingPairs  map[string]keyPointPair
}

func (t *Merging) Set(key string, point pointT) error {
	if t.b.Meta.PointSize != point.size() {
		return ErrPointSize
	}
	t.pendingPairs[key] = keyPointPair{key: key, point: point}
	return nil
}

// Commit the merging
// 0. write+sync `append-log` (append points)
// 1. create merge file to lock target version
// 2.write merge info to the merge file and sync it.
// then merging process can be retry with it in future
// 3.merge the merge file with the data file of current version to a new version data file
// 4.update the *Bucket meta info and rewrite it to disk. end
func (t *Merging) Commit(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	mergeFile, err := createMergeFile(t.b.dirPath)
	if err != nil {
		if os.IsExist(err) {
			return ErrMergeLockExists
		}
		return errors.WithStack(err)
	}
	// never to close many times
	defer mergeFile.Close()

	// |point|......| new keys chunk | point count(uint32)|
	mergeInfoBuf := bytes.NewBuffer(make([]byte, 0, len(t.pendingPairs)*t.b.PointSize))
	for _, idx := range t.b.ascKeys {
		pair, ok := t.pendingPairs[idx.key]
		if ok {
			delete(t.pendingPairs, idx.key)
			_, err := mergeInfoBuf.Write([]byte(pair.point))
			if err != nil {
				return errors.WithStack(err)
			}
		} else {
			// point is absent
			_, err := mergeInfoBuf.Write(newEmptyPoint(t.b.PointSize))
			if err != nil {
				return errors.WithStack(err)
			}
		}
	}

	pointCount := len(t.b.ascKeys)
	//newKeys := make([]keyIDX, 0, len(t.pendingPairs))
	// new keys chunk :|key-length(uint16)|key(n bytes)|panelNo(uint32)|...
	for key, pair := range t.pendingPairs {
		idx := keyIDX{key: key, panelNo: pointCount}
		_, err := mergeFile.Write([]byte(pair.point))
		if err != nil {
			return errors.WithStack(err)
		}
		err = binary.Write(mergeInfoBuf, binary.BigEndian, uint16(len(idx.key)))
		if err != nil {
			return errors.WithStack(err)
		}
		_, err = mergeInfoBuf.Write([]byte(idx.key))
		if err != nil {
			return errors.WithStack(err)
		}
		err = binary.Write(mergeInfoBuf, binary.BigEndian, uint32(idx.panelNo))
		if err != nil {
			return errors.WithStack(err)
		}
		//newKeys = append(newKeys, idx)
		//newPoints = append(newPoints, pair.point)
		pointCount += 1
	}
	err = binary.Write(mergeInfoBuf, binary.BigEndian, uint32(pointCount))
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = mergeFile.Write(mergeInfoBuf.Bytes())
	if err != nil {
		return errors.WithStack(err)
	}
	err = mergeFile.Sync()
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (t *Merging) merge() {
}

type Bucket struct {
	Meta
	dirPath string
	ascKeys []keyIDX
	group   groupT
}

func (b *Bucket) Begin() *Merging {
}
