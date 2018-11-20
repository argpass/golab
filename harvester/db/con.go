package db

import "github.com/argpass/golab/harvester/libs"

type IsDBCon interface {
	// Save
	Save(entry *libs.Entry) error
	// Query on the connection to the db by a special doc_type
	Query() (Query)
}

