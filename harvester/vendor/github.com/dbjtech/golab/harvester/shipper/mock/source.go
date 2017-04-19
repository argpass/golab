package mock

import (
	"github.com/pkg/errors"
	"io/ioutil"
	"github.com/dbjtech/golab/harvester/libs"
	"math/rand"
	"fmt"
	"math"
	"time"
)

func randomNumString(length int) string {
	i := rand.Intn(int(math.Pow10(length)))
	token := fmt.Sprintf("%%0.%dd", length)
	return fmt.Sprintf(token, i)
}

type LogMocker struct {
	rows []string
	snRandLen int
	tagRandLen int
	
	tags map[string]struct{}
	terms map[string]struct{}
	sns map[string]struct{}
}

func NewLogMocker(filepath string, maxRowCount int) (*LogMocker, error) {
	if maxRowCount == 0 {
		maxRowCount = math.MaxInt32
	}
	rows, err := readRows(filepath, maxRowCount)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, errors.New("empty rows")
	}
	mocker := &LogMocker{
		rows:rows, snRandLen: 5, tagRandLen:4,
		tags:make(map[string]struct{}),
		terms:make(map[string]struct{}),
		sns:make(map[string]struct{}),
	}
	return mocker, nil
}

func (m *LogMocker) randomSN() (string)  {
	v := fmt.Sprintf("sn_%s", randomNumString(m.snRandLen))
	m.sns[v] = struct{}{}
	return v
}

func (m *LogMocker) randomTag() (string)  {
	v := fmt.Sprintf("tag_%s", randomNumString(m.tagRandLen))
	m.tags[v] = struct{}{}
	return v
}

func (m *LogMocker) randomTerm() (string, string)  {
	v := fmt.Sprintf("v_%s", randomNumString(m.tagRandLen))
	k := fmt.Sprintf("k_%s", randomNumString(3))
	m.terms[k] = struct{}{}
	return k, v
}

func (m *LogMocker) randomRow() string {
	return m.rows[rand.Intn(len(m.rows))]
}

func (m *LogMocker) RandomEntry() (et *libs.Entry) {
	t := time.Now().Unix()
	sn := m.randomSN()
	body := m.randomRow()
	et = &libs.Entry{
		Timestamp:uint64(time.Now().Unix()),
		Body: body,
		Type:"mock",
		Fields:map[string]libs.Value{},
	}
	et.AddStringTag("mock")
	// attatch 4 fields and 4 random tags
	for i:=0; i < 3; i++ {
		et.AddStringTag(m.randomTag())
		k, v := m.randomTerm()
		et.AddStringField(k, v)
	}
	et.AddStringField("sn", sn)
	et.AddIntField("t", t)
	return et
}

func readRows(filepath string, maxRowsCnt int) ([]string, error) {
	data, err := ioutil.ReadFile(filepath)
	if err != nil {
		return nil, errors.Wrap(err, "read file data")
	}
	var rows []string
	var rowBuf []byte
	for _, c := range data {
		if len(rows) >= maxRowsCnt {
			break
		}
		if c == '[' && rowBuf != nil {
			// wrap pre row
			rows = append(rows, string(rowBuf))
			rowBuf = nil
		}else{
			rowBuf = append(rowBuf, c)
		}
	}
	if rowBuf != nil && maxRowsCnt > len(rows) {
		rows = append(rows, string(rowBuf))
		rowBuf = nil
	}
	return rows, nil
}
