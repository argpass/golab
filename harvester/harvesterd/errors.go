package harvesterd

import "fmt"

var _ error = Error{}

type Error struct {
	Code int
}

func (e Error) Error() string {
	return fmt.Sprintf("E_%d", e.Code)
}
