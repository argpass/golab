package ari

import (
	"fmt"
	"strings"
	"github.com/pkg/errors"
)


func getEsRawIdx(db string, shard string) string {
	index := fmt.Sprintf("ari.%s.%s", db, shard)
	return index
}

func unpackIdxName(idx string) (db string, shard string, err error) {
	sp := strings.Split(idx, ".")
	if len(sp) != 3 {
		err = errors.New("invalid idx name")
		return
	}
	db, shard = sp[1], sp[2]
	return
}

func getESRDIdx(db string) string {
	index := fmt.Sprintf("ari.%s_r", db)
	return index
}
