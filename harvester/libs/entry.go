package libs


var ValueTypes = struct {
	INT  int
	STR  int
	FLOAT int
}{0,1,2}

type Value struct {
	Type int

	IVal int64
	SVal string
	FVal float64
}

type Entry struct {
	Fields 		map[string]Value
	Tags   		[]Value

	Type      	string
	// Timestamp, collected timestamp(seconds)
	Timestamp 	uint64
	Body      	string
}

func NewEntry(entryType string, timestamp uint64, body string) *Entry {
	return &Entry{
		Type:entryType,
		Body:body,
		Timestamp:timestamp,
		Fields:map[string]Value{},
	}
}

func (e *Entry) AddIntField(key string, v int64) {
	e.Fields[key] = Value{Type:ValueTypes.INT, IVal:v}
}

func (e *Entry) AddFloatField(key string, v float64) {
	e.Fields[key] = Value{Type:ValueTypes.FLOAT, FVal:v}
}

func (e *Entry) AddStringField(key string, v string){
	e.Fields[key] = Value{Type:ValueTypes.STR, SVal:v}
}

func (e *Entry) AddStringTag(tag string) {
	e.Tags = append(e.Tags, Value{Type:ValueTypes.STR, SVal:tag})
}

func (e *Entry) AddIntTag(tag int64) {
	e.Tags = append(e.Tags, Value{Type:ValueTypes.STR, IVal:tag})
}

func (e *Entry) AddFloatTag(tag float64) {
	e.Tags = append(e.Tags, Value{Type:ValueTypes.FLOAT, FVal:tag})
}

