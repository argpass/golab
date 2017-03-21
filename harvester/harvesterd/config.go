package harvesterd

type DbRouting struct {
	SaveToDb    string      `json:"save_to_db"`
}

type Config struct {
	TypeRouting   map[string]DbRouting  `json:"type_routing"`
}
