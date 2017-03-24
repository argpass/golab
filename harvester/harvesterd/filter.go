package harvesterd

import "github.com/dbjtech/golab/harvester/libs"

type IsFilter interface {
	Filter(entry *libs.Entry) (bool, error)
}
