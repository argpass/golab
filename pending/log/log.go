package log

var FieldTypes = struct {
	INT  int
	STR  int
	OBJ  int
	TAG  int
}{0,1,2,3}

type Field struct {
	vType int
	key string

	iVal int64
	sVal string
	obj interface{}
}

func Int(key string, val int64) Field {
	return Field{key:key, vType:FieldTypes.INT, iVal:int64(val)}
}

func Str(key string, val string) Field {
	return Field{key:key, vType:FieldTypes.STR, sVal:val}
}

func Obj(key string, val interface{}) Field {
	return Field{key:key, vType:FieldTypes.OBJ, obj:val}
}

func Tag(key string) Field {
	return Field{key:key, vType:FieldTypes.TAG}
}


const (
	DEBUG = (iota + 1) * 10  // 10
	INFO			 // 20
	WARN 			 // 30
	ERROR 			 // 40
	FATAL 			 // 50
)

type IsLogger interface {
	Info(msg string, fields ...Field)
	Debug(msg string, fields ...Field)
	Warn(msg string, fields ...Field)
	Error(msg string, fields ...Field)
	Fatal(msg string, fields ...Field)

	With(field Field, fields ...Field) IsLogger
}

type logger struct {
	// level
	lv int
	fieldsMap map[string]Field
}

func (logger *logger) Info(msg string, fields ...Field)() {
}

func (logger *logger) Debug(msg string, fields ...Field)() {
}

func (logger *logger) Warn(msg string, fields ...Field)() {
}

func (logger *logger) Error(msg string, fields ...Field)() {
}

func (logger *logger) Fatal(msg string, fields ...Field)() {
}

func (logger *logger) clone() *logger {
	dup := *logger
	newLogger := &dup

	// setup new meta
	newLogger.fieldsMap = make(map[string]Field)
	// copy old fields map
	if logger.fieldsMap != nil {
		for k, v := range logger.fieldsMap {
			newLogger.fieldsMap[k] = v
		}
	}
	return newLogger
}

func (logger *logger) With(field Field, fields ...Field) IsLogger {
	// add new fields
	newLogger := logger.clone()
	newLogger.fieldsMap[field.key] = field
	for _, f := range fields {
		newLogger.fieldsMap[f.key] = f
	}
	return newLogger
}

func (logger *logger) log(lv int, msg string, fields ...Field) {
	if lv < logger.lv {
		// level is too low, ignore it
		return
	}
	// todo: add entry to entry pool
}

func NewLogger(level int) IsLogger {
	return &logger{lv:level}
}

