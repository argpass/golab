package db

import "github.com/dbjtech/golab/harvester/harvesterd"


type IsDBCon interface {
	// Save
	Save(entry *harvesterd.Entry) error
	// Query on the connection to the db by a special doc_type
	Query() (*Query)
}

