package internal

import "github.com/argpass/golab/mass/client"

type IsBeatRepository interface {
	Save(b *Beat) error
}

var _ IsBeatRepository = &massRepoImpl{}

type massRepoImpl struct {
	con client.IsDBConnection
}

func (m *massRepoImpl) Save(b *Beat) error {
	return nil
}
