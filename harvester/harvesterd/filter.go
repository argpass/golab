package harvesterd

type IsFilter interface {
	Filter(entry *Entry) (bool, error)
}
