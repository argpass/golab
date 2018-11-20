package harvesterd

import "github.com/argpass/golab/harvester/libs"

type IsFilter interface {
	Filter(entry *libs.Entry) (bool, error)
}
