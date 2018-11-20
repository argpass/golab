package internal

type IsPolicy interface {

	// Decode create a new policy instance with decoding last result `data`
	Decode(data []byte) (IsPolicy, error)

	// Merge newest locations to calculating new result
	Merge(location []Location) error

	// Encode the result
	Encode() ([]byte, error)
}
