package mock

import (
	"io/ioutil"
	"github.com/pkg/errors"
	"fmt"
	"os"
	"encoding/json"
)

type Config struct {
	MockFile    string      `config:"mock_file"`
	BatchNum    int         `config:"batch_num"`
	MaxRowCount int         `config:"max_row_count"`
	DumpFile    string      `config:"dump_file"`
}

type Status struct {
	Size        int         `json:"size"`
	Num         int         `json:"num"`
	Cost        int         `json:"cost"`
}

type StatusAware struct {
	dumpFile string
	status *Status
}

func pathExist(path string) (bool, error) {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}else{
			return false, errors.Wrap(err, fmt.Sprintf("stat file %s", path))
		}
	}
	return true, nil
}

func NewStatusAware(dumpFile string) (*StatusAware, error) {
	exists, err := pathExist(dumpFile)
	if err != nil {
		return nil, err
	}
	
	s := &StatusAware{}
	if exists {
		data, err := ioutil.ReadFile(dumpFile)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("read dump file %s", dumpFile))
		}
		var status Status
		err = json.Unmarshal(data, &status)
		if err != nil {
			return nil, errors.Wrap(err, "loads json")
		}
		s.status = &status
	}
	return s, nil
}

func (s *StatusAware) Flush() error {
	data, err := json.Marshal(s.status)
	if err != nil {
		return errors.Wrap(err, "marshal status")
	}
	err = ioutil.WriteFile(s.dumpFile, data, 0666)
	if err != nil {
		return errors.Wrap(err, "write dump file")
	}
	return nil
}

var defaultConf = Config{
	MockFile:"mock.log",
	BatchNum: 800000,
	MaxRowCount: 200000,
	DumpFile: "mock.dat",
}

func NewDefaultConfig() Config {
	cfg := defaultConf
	return cfg
}

