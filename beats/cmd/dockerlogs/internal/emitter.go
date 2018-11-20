package internal

type Emitter interface {
	Emit(b *Beat) error
}

type EmitterFunc func(b *Beat) error

func (fn EmitterFunc) Emit(b *Beat) error {
	return fn(b)
}
