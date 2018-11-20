package internal

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func ReadLine(fileName string, handler func(string)) error {
	f, err := os.Open(fileName)
	if err != nil {
		return err
	}
	buf := bufio.NewReader(f)
	for {
		line, err := buf.ReadString('\n')
		line = strings.TrimSpace(line)
		handler(line)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
	return nil
}

func TestNewLocation(t *testing.T) {
	dis := CalculateDistance(40, 120, 40, 120.0000001)
	t.Logf("dis:%.6f\n", dis)
}

func TestCalDensityPeeks(t *testing.T) {
	// 复现特定hash区域
	//useHash := "w7w99" // 聚类明显
	//w7w93-53 个点，w7mb1-162 聚集明显
	// w7jzg
	useHash := "w7mb1"
	//useHash := "w7jzg" // 聚集不明显
	//useHash := "w7w93"
	//useHash := "w7w99"
	fi, err := os.OpenFile("/tmp/density.csv", os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	defer fi.Close()
	maximum := 20000
	path := "/tmp/offline_lng_lat_time.csv"
	geoMap := make(GeoMap)
	curLID := 0
	err = ReadLine(path, func(s string) {
		if curLID >= maximum {
			return
		}
		if len(s) == 0 {
			return
		}
		curLID += 1
		tokens := strings.Split(s, ",")
		if len(tokens) != 3 {
			t.Logf("invalid line:`%s`\n", s)
			return
		}
		lng, err := strconv.ParseFloat(tokens[0], 32)
		if err != nil {
			t.Logf("err on line:`%s`\n", s)
			panic(err)
		}
		lat, err := strconv.ParseFloat(tokens[1], 32)
		if err != nil {
			t.Logf("err on line:`%s`\n", s)
			panic(err)
		}
		if math.Dim(lat, 0) == 0 || math.Dim(lng, 0) == 0 {
			//t.Logf("recv zero location, ignore it, line:`%s`\n", s)
			return
		}
		location := NewLocation(curLID, "", "", lat, lng)
		locations, ok := geoMap[location.GeoHash]
		if !ok {
			locations = make([]Location, 0, maximum)
		}
		locations = append(locations, location)
		geoMap[location.GeoHash] = locations
	})
	if err != nil {
		panic(err)
	}
	t.Logf("recv geomap size:%d\n", len(geoMap))

	if gm, ok := geoMap[useHash]; ok {
		if len(gm) > 0 {
			t.Logf("%s->%d\n", useHash, len(gm))
			begin := time.Now()
			result := CalDensityPeeks(gm)
			t.Logf("done cost:%v, maximum_dis:%.0f\n", time.Now().Sub(begin), result.MaximumDistance)
			for _, c := range result.Clusters {
				t.Logf("cluster:%d, density:%.5f, points:%d\n", c.CenterPoint.Lid, c.Density, len(c.Points))
			}
			_, err = fmt.Fprint(fi, "cluster,lid,density,dis,lat,lng\n")
			if err != nil {
				panic(err)
			}
			for _, idx := range result.SortedIndexes {
				_, err = fmt.Fprintf(fi, "%d,%d,%.5f,%.0f,%.5f,%.5f\n",
					idx.CenterLID, idx.Lid, idx.Density, idx.DistanceMinimum, idx.Lat, idx.Lng)
				if err != nil {
					panic(err)
				}
			}
		}
	}
	//CalDensityPeeks(locations)
}
