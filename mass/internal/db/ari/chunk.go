package ari

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/pkg/errors"
)

const (
	COMPRESSED_NO   uint16 = 0
	COMPRESSED_GZIP uint16 = 1
)

// ChunkID locates a chunk by fd and offset
// +--- fd ---|--- offset ---+
// | 16 bits  |  48 bits     |
// +-------------------------+
type ChunkID uint64

func MakeChunkID(fd uint16, offset uint64) ChunkID {
	return ChunkID((uint64(fd) << 48) | ((^uint64(0) >> 16) & offset))
}

func (key ChunkID) FD() uint16 {
	d := uint64(key)
	return uint16(d >> 48)
}

func (key ChunkID) Offset() uint64 {
	d := uint64(key)
	return (^uint64(0) >> 16) & d
}

// RowKey 10 bytes
// +-----ChunkID-----+----Serial Number---+
// |	64 bits      |	16 bits  	      |
// +--------------------------------------+
// It means that there are at most 65535 log items in a single chunk
type RowKey struct {
	ChunkID ChunkID
	Serial  uint16
}

func MakeRowKey(fd uint16, off uint64, serial uint16) RowKey {
	cid := MakeChunkID(fd, off)
	return RowKey{ChunkID: cid, Serial: serial}
}

// EncodeBase64 encodes row key bytes to base64 string
func (r RowKey) EncodeBase64() string {
	s := base64.StdEncoding.EncodeToString(r.Bytes())
	return s
}

func (r RowKey) Bytes() []byte {
	var w bytes.Buffer
	binary.Write(&w, binary.BigEndian, r)
	return w.Bytes()
}

func UnpackRowKey(data []byte) (RowKey, error) {
	var key RowKey
	var r = bytes.NewReader(data)
	err := binary.Read(r, binary.BigEndian, &key)
	if err != nil {
		return key, err
	}
	return key, nil
}

func UnpackRowKeyByBase64(bs64 string) (RowKey, error) {
	data, err := base64.StdEncoding.DecodeString(bs64)
	if err != nil {
		return RowKey{}, err
	}
	return UnpackRowKey(data)
}

// CKHeader
type CKHeader struct {
	DataSize   uint32
	Compressed uint16
}

// Chunk format
//
// >--------------chunk header----------<
//
// +------------------------------------+---------------+
// |	data size | compressed flag     |	packed data |
// |--------------+---------------------+---------------|
// |    4bytes    |   2 bytes       	|  n bytes      |
// +----------------------------------------------------+
//
type Chunk struct {
	CKHeader

	// unpacked data format:
	//
	// >------------------header------------<
	//
	// +--------------------+-------------------+-----------+-------- - - +---------+
	// |	header size	    |	row offset list	|	row#1	|   row#2	  |	row#n	|
	// +--------------------+-------------------+-----------+-------- - - +---------+
	// |	4bytes		    |	UVarInt..   	|    n bytes|  n bytes	  |	n bytes	|
	// +--------------------+-------------------+-----------+-------- - - +---------+
	packed []byte
}

func (c *Chunk) ResolveRows(serialsMap map[uint16][]byte) error {
	if len(serialsMap) == 0 {
		return nil
	}
	data, err := c.Unpack()
	if err != nil {
		return errors.Wrap(err, "unpack chunk")
	}
	// header size
	hdSize := int(binary.BigEndian.Uint32(data[:4]))
	var hdReader = bytes.NewReader(data[4 : 4+hdSize])

	var last int = 0
	var remainCnt = len(serialsMap)
	rowsData := data[4+hdSize:]
	for i := 0; ; i++ {
		rowLen, err := binary.ReadUvarint(hdReader)
		if err != nil {
			if strings.EqualFold(err.Error(), "EOF") {
				break
			}
			return errors.Wrap(err, "read chunk's data uvarint")
		}
		end := int(rowLen) + last
		if _, yes := serialsMap[uint16(i)]; yes {
			serialsMap[uint16(i)] = rowsData[last:end]
			remainCnt--
		}
	}
	// check if all row resolved
	if remainCnt == 0 {
		return nil
	}
	return errors.New("fail to fetch all rows")
}

// Unpack chunk packed data
func (c *Chunk) Unpack() ([]byte, error) {
	if c.Compressed == COMPRESSED_NO {
		return c.packed, nil
	}
	if c.Compressed == COMPRESSED_GZIP {
		var br = bytes.NewBuffer(c.packed)
		gr, err := gzip.NewReader(br)
		if err != nil {
			return nil, errors.Wrap(err, "new gizp reader from chunck bytes")
		}
		data, err := ioutil.ReadAll(gr)
		if err != nil {
			return nil, errors.Wrap(err, "read all gzip buf")
		}
		return data, nil
	}
	return nil, errors.New(
		fmt.Sprintf("unkown compressed format:%d", c.Compressed))
}

// ReadChunk reads chunk data from the `io.Reader`
func ReadChunk(reader io.Reader) (*Chunk, error) {
	chunk := &Chunk{}
	err := binary.Read(reader, binary.BigEndian, &chunk.CKHeader)
	if err != nil {
		if strings.EqualFold(err.Error(), "EOF") {
			return nil, nil
		}
		return nil, errors.Wrap(err, "read chunk header")
	}

	// resolve the packed
	packedSize := chunk.DataSize
	chunk.packed = make([]byte, packedSize)
	n, err := io.ReadFull(reader, chunk.packed)
	if err != nil {
		return nil, errors.Wrapf(err,
			"read packed data, expect %d , read %d, err:%s",
			packedSize, n, err.Error())
	}
	return chunk, nil
}

// Bytes returns bytes of the chunk
func (c *Chunk) Bytes() []byte {
	var hw = bytes.NewBuffer(nil)
	binary.Write(hw, binary.BigEndian, c.CKHeader)
	hw.Write(c.packed)
	return hw.Bytes()
}

type ChunkBuilder struct {
	varintBuf  []byte
	lenListBuf bytes.Buffer
	rowsBuf    bytes.Buffer
	dataSize   int
}

func NewChunkBuilder() *ChunkBuilder {
	b := &ChunkBuilder{
		varintBuf: make([]byte, 8),
	}
	return b
}

func (b *ChunkBuilder) AddRow(row []byte) {
	l := uint64(len(row))
	b.dataSize += len(row)
	used := binary.PutUvarint(b.varintBuf, l)
	b.lenListBuf.Write(b.varintBuf[0:used])
	b.rowsBuf.Write(row)
}

func (b *ChunkBuilder) Build() *Chunk {
	var chunk *Chunk
	var buf bytes.Buffer

	// write header size
	binary.Write(&buf, binary.BigEndian, uint32(b.lenListBuf.Len()))
	buf.Write(b.lenListBuf.Bytes())
	buf.Write(b.rowsBuf.Bytes())

	// compress buf
	var gw bytes.Buffer
	w := gzip.NewWriter(&gw)
	w.Write(buf.Bytes())
	w.Close()

	// build chunk
	if compressedRatio := float64(gw.Len()) / float64(buf.Len()); compressedRatio <= 0.8 {
		chunk = &Chunk{
			CKHeader: CKHeader{
				Compressed: COMPRESSED_GZIP,
			},
			packed: gw.Bytes(),
		}
	} else {
		chunk = &Chunk{
			CKHeader: CKHeader{
				Compressed: COMPRESSED_NO,
			},
			packed: buf.Bytes(),
		}
	}
	chunk.DataSize = uint32(len(chunk.packed))
	return chunk
}
