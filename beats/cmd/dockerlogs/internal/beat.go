package internal

import (
	"io"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type Beat struct {
	mu        sync.RWMutex
	Type      string                 `json:"type"`
	Body      string                 `json:"body"`
	CreatedAt int64                  `json:"created_at"`
	Fields    map[string]interface{} `json:"fields"`
	Tags      map[string]struct{}    `json:"tags"`
}

func NewBeat(beatType string, body string) *Beat {
	return &Beat{
		Type:      beatType,
		Body:      body,
		CreatedAt: time.Now().UnixNano(),
		Fields:    make(map[string]interface{}, 10),
		Tags:      make(map[string]struct{}, 10),
	}
}

func (b *Beat) AddField(name string, val interface{}) {
	b.mu.Lock()
	b.Fields[name] = val
	b.mu.Unlock()
}

func (b *Beat) AddTag(tag string) {
	b.mu.Lock()
	b.Tags[tag] = struct{}{}
	b.mu.Unlock()
}

var _ io.WriteCloser = &beater{}

type beater struct {
	beatType         string
	pending          []byte
	buf              []byte
	lastLineIdx      int
	lastLineLen      int
	separatorMatcher func(last []byte, current []byte) bool
	filters          []func(b *Beat) bool
	emitter          Emitter
}

func (w *beater) Close() error {
	return nil
}

// NewBeater create an instance of `Beater`
func NewBeater(
	beatType string,
	emitter Emitter,
	filters []func(b *Beat) bool,
	separatorMatcher func(last []byte, current []byte) bool) io.WriteCloser {
	return &beater{
		beatType:         beatType,
		separatorMatcher: separatorMatcher,
		filters:          filters,
		emitter:          emitter,
	}
}

func (w *beater) emitBeat(b *Beat) error {
	// process filtering to add fields and tags
	for _, fi := range w.filters {
		if !fi(b) {
			break
		}
	}
	return w.emitter.Emit(b)
}

func (w *beater) emitLine(line []byte) (err error) {
	var lastLine []byte
	if w.lastLineLen > 0 {
		lastLine = w.buf[w.lastLineIdx : w.lastLineIdx+w.lastLineLen]
	}
	if w.separatorMatcher(lastLine, line) {
		// previous lines could be wrapped as a Beat
		if len(w.buf) != 0 {
			err = w.emitBeat(NewBeat(w.beatType, string(w.buf)))
		}
		w.buf = w.buf[0:0]
	}
	w.buf = append(w.buf, line...)
	if err != nil {
		err = errors.Wrapf(err, "on emitting line `%s`", line)
		return
	}
	// check buf if maximum size exceeded
	// fixme: to handle maximum size exceeded error
	//if len(w.buf) >= 1024*128 {
	//	panic(errors.New("buf exceeds maximum size"))
	//}
	return
}

// Write accepts stream data
func (w *beater) Write(p []byte) (n int, err error) {
	n = len(p)
	w.pending = append(w.pending, p...)
	p = w.pending
	// todo: sample size to determine buf cap
	w.pending = w.pending[0:0:len(p)]
scanLine:
	for i, b := range p {
		if b == '\n' {
			line := p[0 : i+1]
			p = p[i+1:]
			err = w.emitLine(line)
			if err != nil {
				return 0, err
			}
			goto scanLine
		}
	}
	w.pending = append(w.pending, p...)
	return
}
