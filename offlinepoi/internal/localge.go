package internal

import (
	"math"
)

const (
	a       = 6378245.0
	ee      = 0.00669342162296594323
	xmPI    = math.Pi * 3000.0 / 180.0
	epsilon = 0.000001
)

func equal(a, b float64) bool {
	return (a-b) < epsilon && (b-a) < epsilon
}

// CalculateDistance 计算两个点之间的距离
func CalculateDistance(lat1, lng1, lat2, lng2 float64) float64 {
	if equal(lat1, lat2) && equal(lng2, lng1) {
		return 0
	}
	unit := math.Pi / 180.0
	radLat1 := lat1 * unit
	radLat2 := lat2 * unit
	radLng1 := lng1 * unit
	radLng2 := lng2 * unit

	a := 6378137.0
	b := 6356752.3142
	f := (a - b) / a
	aSqMinusBSqOverBSq := (a*a - b*b) / (b * b)

	L := radLng2 - radLng1
	A := 0.0
	U1 := math.Atan((1.0 - f) * math.Tan(radLat1))
	U2 := math.Atan((1.0 - f) * math.Tan(radLat2))

	cosU1 := math.Cos(U1)
	cosU2 := math.Cos(U2)
	sinU1 := math.Sin(U1)
	sinU2 := math.Sin(U2)
	cosU1CosU2 := cosU1 * cosU2
	sinU1SinU2 := sinU1 * sinU2
	sigma := 0.0
	deltaSigma := 0.0
	lambda := L
	for i := 0; i < 20; i++ {
		lambdaOrig := lambda
		cosLambda := math.Cos(lambda)
		sinLamda := math.Sin(lambda)
		t1 := cosU2 * sinLamda
		t2 := cosU1*sinU2 - sinU1*cosU2*cosLambda
		sinSqSigma := t1*t1 + t2*t2
		sinSigma := math.Sqrt(sinSqSigma)
		cosSigma := sinU1SinU2 + cosU1CosU2*cosLambda
		sigma = math.Atan2(sinSigma, cosSigma)
		sinAlpha := 0.0
		if !equal(sinSigma, 0.0) {
			sinAlpha = cosU1CosU2 * sinLamda / sinSigma
		}
		cosSqAlpha := 1.0 - sinAlpha*sinAlpha
		cos2SM := 0.0
		if !equal(cosSqAlpha, 0) {
			cos2SM = cosSigma - 2.0*sinU1SinU2/cosSqAlpha
		}
		uSquared := cosSqAlpha * aSqMinusBSqOverBSq
		A = 1 + (uSquared/16384.0)*(4096.0+uSquared*(-768+uSquared*(320.0-175.0*uSquared)))
		B := (uSquared / 1024.0) * (256.0 + uSquared*(-128.0+uSquared*(74.0-47.0*uSquared)))
		C := (f / 16.0) * cosSqAlpha * (4.0 + f*(4.0-3.0*cosSqAlpha))
		cos2SMSq := cos2SM * cos2SM
		deltaSigma = B * sinSigma * (cos2SM + (B/4.0)*(cosSigma*
			(-1.0+2.0*cos2SMSq)-(B/6.0)*cos2SM*
			(-3.0+4.0*sinSigma*sinSigma)*(-3.0+4.0*cos2SMSq)))
		lambda = L + (1.0-C)*f*sinAlpha*(sigma+C*sinSigma*
			(cos2SM+C*cosSigma*(-1.0+2.0*cos2SM*cos2SM)))
		if !equal(lambda, 0.0) {
			delta := (lambda - lambdaOrig) / lambda
			if math.Abs(delta) < 1.0*math.Pow10(-12) {
				break
			}
		} else {
			break
		}
	}
	distance := b * A * (sigma - deltaSigma)
	return distance
}

func GeoParse(lat float64, lng float64) (clat float64, clng float64, err error) {
	glat, glng := Wgs2gcj(lat, lng)
	elat, elng := bd_encrypt(glat, glng)
	return elat, elng, nil
}

//地球坐标系 (WGS-84) 转换为火星坐标系 (GCJ-02)
func Wgs2gcj(lat float64, lng float64) (clat float64, clng float64) {
	if outOfChina(lat, lng) {
		return 0, 0
	}
	d_lat := transform_lat(lng-105.0, lat-35.0)
	d_lng := transform_lng(lng-105.0, lat-35.0)
	rad_lat := lat / 180.0 * math.Pi
	magic := math.Sin(rad_lat)
	magic = 1 - ee*magic*magic
	sqrt_magic := math.Sqrt(magic)
	d_lat = (d_lat * 180.0) / ((a * (1 - ee)) / (magic * sqrt_magic) * math.Pi)
	d_lng = (d_lng * 180.0) / (a / sqrt_magic * math.Cos(rad_lat) * math.Pi)
	return lat + d_lat, lng + d_lng
}

// 火星坐标系 (GCJ-02)转换为地球坐标系 (WGS-84)
func gcj2wgs(lat float64, lng float64) (clat float64, clng float64) {
	if outOfChina(lat, lng) {
		return 0, 0
	}
	tlat, tlng := Wgs2gcj(lat, lng)
	return 2*lat - tlat, 2*lng - tlng
}

func outOfChina(lat float64, lng float64) (isOutOfChina bool) {
	if lng < 72.004 || lng > 137.8347 {
		return true
	}
	if lat < 0.8293 || lat > 55.8271 {
		return true
	}
	return false
}

func transform_lat(x float64, y float64) (clat float64) {
	ret := -100.0 + 2.0*x + 3.0*y + 0.2*y*y + 0.1*x*y + 0.2*math.Sqrt(math.Abs(x))
	ret += (20.0*math.Sin(6.0*x*math.Pi) + 20.0*math.Sin(2.0*x*math.Pi)) * 2.0 / 3.0
	ret += (20.0*math.Sin(y*math.Pi) + 40.0*math.Sin(y/3.0*math.Pi)) * 2.0 / 3.0
	ret += (160.0*math.Sin(y/12.0*math.Pi) + 320*math.Sin(y*math.Pi/30.0)) * 2.0 / 3.0
	return ret
}

func transform_lng(x float64, y float64) (clng float64) {
	ret := 300.0 + x + 2.0*y + 0.1*x*x + 0.1*x*y + 0.1*math.Sqrt(math.Abs(x))
	ret += (20.0*math.Sin(6.0*x*math.Pi) + 20.0*math.Sin(2.0*x*math.Pi)) * 2.0 / 3.0
	ret += (20.0*math.Sin(x*math.Pi) + 40.0*math.Sin(x/3.0*math.Pi)) * 2.0 / 3.0
	ret += (150.0*math.Sin(x/12.0*math.Pi) + 300.0*math.Sin(x/30.0*math.Pi)) * 2.0 / 3.0
	return ret
}

// 火星坐标系(GCJ-02)坐标转换成百度坐标系(BD-09)坐标
func bd_encrypt(x float64, y float64) (elat float64, clng float64) {
	if outOfChina(x, y) {
		return 0, 0
	}
	z := math.Sqrt(x*x+y*y) + 0.00002*math.Sin(y*xmPI)
	theta := math.Atan2(y, x) + 0.000003*math.Cos(x*xmPI)
	return z*math.Cos(theta) + 0.0065, z*math.Sin(theta) + 0.006
}

//百度坐标系 (BD-09)坐标转换成火星坐标系(GCJ-02)坐标
func bd_decrypt(x float64, y float64) (elat float64, clng float64) {
	x -= 0.0065
	y -= 0.006
	z := math.Sqrt(x*x+y*y) - 0.00002*math.Sin(y*xmPI)
	theta := math.Atan2(y, x) - 0.000003*math.Cos(x*xmPI)
	return z * math.Cos(theta), z * math.Sin(theta)
}
