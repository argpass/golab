package libs

import "fmt"

type Error struct {
	Err         error
	Code        int
	Message     string
	IsFatal    bool
}

func (e Error) Error() string {
	return fmt.Sprintf("Error(%d, %v) err:%v, msg:%s ", e.Code, e.IsFatal, e.Err, e.Message)
}
 
