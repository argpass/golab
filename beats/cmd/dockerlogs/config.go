package main

//type SourceConfig struct {
//	OutPut []map[string]interface{}
//}
//
//type Config struct {
//	// Sources map[Type]SourceConfig
//	Sources map[string]SourceConfig `json:"sources"`
//}

type MassConfig struct {
}

type RotateConfig struct {
	Backups     int `json:"backups"`
	SizeMaximum int `json:"size_maximum"`
}

type MassOutPut struct {
	MassOutPut
	Type string `json:"type"`
}

type RotateOutPut struct {
	RotateConfig
	Type string `json:"type"`
}

type OutPutConfig struct {
	Masses       map[string]MassOutPut   `json:"masses"`
	LocalRotates map[string]RotateConfig `json:"local_rotates"`
}

type Config struct {
	OutPut OutPutConfig `json:"output"`
}
