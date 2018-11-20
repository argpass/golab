package internal

import (
	"context"

	"github.com/gansidui/geohash"
)

const (
	hashPrecise = 5
)

type IsKeyValueDB interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, value []byte) error
}

// GeoMap geo_hash => []Point
type GeoMap map[string][]Location

type Location struct {
	GeoHash string  `json:"geo_hash"`
	LID     int     `json:"lid"`
	Lat     float64 `json:"lat"`
	Lng     float64 `json:"lng"`
	SN      string  `json:"sn"`
	TID     string  `json:"tid"`
}

func (l Location) ReHash(precise int) string {
	hash, _ := geohash.Encode(l.Lat, l.Lng, precise)
	return hash
}

func NewLocation(lid int, sn string, tid string, lat float64, lng float64) Location {
	hash, _ := geohash.Encode(lat, lng, hashPrecise)
	return Location{
		LID:     lid,
		SN:      sn,
		TID:     tid,
		Lat:     lat,
		Lng:     lng,
		GeoHash: hash,
	}
}

func (l Location) CalculateDistance(loc Location) float64 {
	lat1, lng1 := l.Lat, l.Lng
	lat2, lng2 := loc.Lat, loc.Lng
	distance := CalculateDistance(lat1, lng1, lat2, lng2)
	return distance
}
