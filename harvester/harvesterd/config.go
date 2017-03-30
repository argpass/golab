package harvesterd

type DbRouting struct {
	SaveToDb    string      `config:"save_to_db"`
}

type Config struct {
	TypeRouting   map[string]DbRouting  `config:"type_routing"`
}
