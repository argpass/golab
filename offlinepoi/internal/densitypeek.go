package internal

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"sort"

	"github.com/pkg/errors"
)

type clusterT struct {
	locations []Location
}

type resultT struct {
	hash        string
	pointNum    int
	clusters    []clusterT
	noisyPoints []Location
}

func (r *resultT) mergeLocations(newest []Location) []Location {
	if r.pointNum == 0 && len(newest) == 0 {
		return nil
	}
	locations := make([]Location, 0, len(newest)+r.pointNum)
	for _, c := range r.clusters {
		locations = append(locations, c.locations...)
	}
	locations = append(locations, r.noisyPoints...)
	locations = append(locations, newest...)
	return locations
}

func loadResult(data []byte) (*resultT, error) {
	// fixme:
	rd := bufio.NewReader(bytes.NewReader(data))
	for {
		line, err := rd.ReadString('\n')
		if len(line) != 0 {
			// fixme:
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, errors.WithStack(err)
		}
	}
	return nil, nil
}

func decodePolicy(lastResultData []byte) (*densityPeekPolicy, error) {
	policy := &densityPeekPolicy{}
	if len(lastResultData) == 0 {
		return policy, nil
	}
	result, err := loadResult(lastResultData)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	policy.result = result
	return policy, nil
}

var _ IsPolicy = &densityPeekPolicy{}

type densityPeekPolicy struct {
	result *resultT
}

func (p *densityPeekPolicy) Decode(lastResultData []byte) (IsPolicy, error) {
	return decodePolicy(lastResultData)
}

func (p *densityPeekPolicy) Merge(newest []Location) error {
	if p.result == nil {
		return errors.New("never to call on uninitialized policy")
	}
	locations := p.result.mergeLocations(newest)
	if len(locations) == 0 {
		return errors.New("there isn't any locations to be calculated")
	}
	p.calculate(locations)
	return nil
}

func (p *densityPeekPolicy) calculate(locations []Location) {
	CalDensityPeeks(locations)
}

func (*densityPeekPolicy) Encode() ([]byte, error) {
	panic("implement me")
}

type distanceMatrix map[string]float64

func (d distanceMatrix) getKey(lidA int, lidB int) string {
	if lidB > lidA {
		return fmt.Sprintf("%d,%d", lidA, lidB)
	}
	return fmt.Sprintf("%d,%d", lidB, lidA)
}

func (d distanceMatrix) Put(lidA int, lidB int, dis float64) {
	key := d.getKey(lidA, lidB)
	d[key] = dis
}

func (d distanceMatrix) Get(lidA int, lidB int) (v float64, ok bool) {
	key := d.getKey(lidA, lidB)
	v, ok = d[key]
	return
}

func gaussianDensity(dis float64, dc int) float64 {
	// density = exp{-(dis/dc)^2}
	return math.Exp(-math.Pow(dis/float64(dc), 2))
}

func easyDensity(dis float64, dc int) float64 {
	if dis < float64(dc) {
		return 1.0
	}
	return 0.0
}

type pointIDX struct {
	Location
	// lid 我的位置点ID
	Lid       int
	CenterLID int
	// isCenterPoint 是否是聚类中心
	isCenterPoint bool
	// density 我的密度值
	Density float64
	// distanceMinimum 比我密度大的点中与我最小距离
	DistanceMinimum float64
	// 我的密度值在密度有序向量中的位置(下标)
	densityPos int
	// distanceMaximum 我与其它点的最大距离
	distanceMaximum float64
}

func CalDensityPeeks(locs []Location) *Result {
	// todo: dc 的计算
	dc := 200
	// 距离矩阵
	distances := make(distanceMatrix, len(locs)*len(locs)/2)
	// map[lid]pointIDX 方便按lid查结果
	idxMap := make(map[int]pointIDX, len(locs))
	// 结果按密度降序，方便找到比某个lid密度高的最小点
	indexes := make([]pointIDX, 0, len(locs))
	result := &Result{}
	for _, loc := range locs {
		if _, ok := idxMap[loc.LID]; ok {
			// ignore repeated location
			continue
		}
		idx := pointIDX{Lid: loc.LID, Location: loc}
		for _, other := range locs {
			if loc.LID == other.LID {
				// skip self
				continue
			}
			dis := loc.CalculateDistance(other)
			idx.Density += gaussianDensity(dis, dc)
			distances.Put(loc.LID, other.LID, dis)
			if dis > idx.distanceMaximum {
				idx.distanceMaximum = dis
			}
			if dis > result.MaximumDistance {
				result.MaximumDistance = dis
			}
		}
		indexes = append(indexes, idx)
		idxMap[loc.LID] = idx
	}
	// 对密度切片进行排序
	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i].Density > indexes[j].Density
	})
	for pos, idx := range indexes {
		var nearby pointIDX
		idx := idxMap[idx.Lid]
		idx.densityPos = pos

		// 查找比我密度大的距离最小的点
		disMinimum := idx.distanceMaximum
		for _, otherIdx := range indexes[:pos] {
			dis, ok := distances.Get(idx.Lid, otherIdx.Lid)
			if !ok {
				panic("distance is absent")
			}
			if dis < disMinimum {
				disMinimum = dis
				nearby = otherIdx
			}
			//// 顺便查找比我密度大且距我最近的中心点
			//if otherIdx.isCenterPoint && dis < myCenterDis {
			//	myCenterLID = otherIdx.lid
			//	myCenterDis = dis
			//}
		}
		idx.DistanceMinimum = disMinimum
		if idx.DistanceMinimum > float64(dc)*2 && idx.Density > 1.0 {
			// center point
			idx.CenterLID = idx.Lid
			idx.isCenterPoint = true
			cluster := &ClusterResult{CenterPoint: idx, Points: []pointIDX{idx}}
			result.Clusters = append(result.Clusters, cluster)
		} else if idx.Density <= 1.0 && idx.DistanceMinimum > float64(dc) {
			// 噪音, 忽略之
		} else {
			myCenterLID := nearby.CenterLID
			cluster := result.FindCluster(myCenterLID)
			if cluster != nil {
				idx.CenterLID = myCenterLID
				cluster.Points = append(cluster.Points, idx)
			}
		}
		idxMap[idx.Lid] = idx
		indexes[pos] = idx
	}
	result.IdxMap = idxMap
	result.SortedIndexes = indexes

	// 计算聚类的整体密度
	for _, c := range result.Clusters {
		hashMap := make(map[string]struct{}, len(c.Points))
		for _, p := range c.Points {
			hashMap[p.ReHash(7)] = struct{}{}
		}
		c.Density = float64(len(hashMap)) / float64(len(c.Points))
	}

	return result
}

// ClusterResult 聚类结果
type ClusterResult struct {
	// CenterPoint 聚类中心点
	CenterPoint pointIDX
	// Density 聚类整体密度
	Density float64
	// Points 聚类中的其它点
	Points []pointIDX
}

// Result 计算结果
type Result struct {
	// CenterDensityMinimum 中心点密度最小阈值
	CenterDensityMinimum float64
	// CenterDistanceCut 中心点距离截断阈值（小于该值则不被选择为中心点)
	CenterDistanceCut int
	// DistanceCut 截断距离，根据此来计算点密度
	DistanceCut int

	MaximumDistance float64
	// IdxMap map[lid]pointIDX
	IdxMap map[int]pointIDX
	// SortedIndexes
	SortedIndexes []pointIDX
	// Clusters 聚类结果
	Clusters []*ClusterResult
}

func (r *Result) FindCluster(lid int) *ClusterResult {
	for _, c := range r.Clusters {
		if c.CenterPoint.Lid == lid {
			return c
		}
	}
	return nil
}
