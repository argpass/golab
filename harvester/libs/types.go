package libs

import "context"

type Starter interface {
	Start(ctx context.Context) error
}
