package utils

var fieldTypes = struct {
	INT64  int
	STR  int
	FLOAT64  int
	BOOL  int
}{0,1,2,3}

type field struct {
	fType int

	iVal int64
	fVal float64
	sVal string
	bVal bool
}

type JsonDict struct {
	data map[string] field
}

func NewJsonDict(raw map[string]interface{}) (JsonDict, error) {
	d := JsonDict{data:map[string]field{}}
	for k, v := range raw {
		fi, err:= d.wrapField(v)
		if err != nil {
			return d, err
		}
		d.data[k] = fi
	}
	return d, nil
}

func (jd JsonDict) SetInt64(key string, val int64) {
	jd.data[key] = field{fType:fieldTypes.INT64, iVal:val}
}

func (jd JsonDict) SetFloat(key string, val float64)  {
	jd.data[key] = field{fType:fieldTypes.FLOAT64, fVal:val}
}

func (jd JsonDict) SetStr(key string, val string)  {
	jd.data[key] = field{fType:fieldTypes.STR, sVal:val}
}

func (jd JsonDict) SetBool(key string, val bool)  {
	jd.data[key] = field{fType:fieldTypes.BOOL, bVal:val}
}

func (jd JsonDict) GetInt64(key string) (val int64, ok bool)  {
	var f field
	f, ok = jd.data[key]
	if ok && f.fType == fieldTypes.INT64{
		val = f.iVal
		return val, ok
	}
	return 0, false
}

func (jd JsonDict) GetFloat64(key string) (val float64, ok bool)  {
	var f field
	f, ok = jd.data[key]
	if ok && f.fType == fieldTypes.FLOAT64{
		val = f.fVal
		return val, ok
	}
	return 0, false
}

func (jd JsonDict) GetStr(key string) (val string, ok bool)  {
	var f field
	f, ok = jd.data[key]
	if ok && f.fType == fieldTypes.STR{
		val = f.sVal
		return val, ok
	}
	return "", false
}

func (jd JsonDict) GetBool(key string) (val bool, ok bool)  {
	var f field
	f, ok = jd.data[key]
	if ok && f.fType == fieldTypes.BOOL{
		val = f.bVal
		return val, ok
	}
	return false, false
}

func (jd JsonDict) wrapField(v interface{}) (field, error) {
	// todo:
	return field{}, nil
}

