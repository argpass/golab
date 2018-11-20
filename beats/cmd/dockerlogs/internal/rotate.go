package internal

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

const (
	defaultSizeMaximum = 50 * 1024 * 1024 // 50MB
	defaultBackups     = 10
)

// RotateWriter
type RotateWriter struct {
	mu sync.Mutex
	fp *os.File

	filename    string
	fpSize      uint32
	maximumSize uint32
	backups     int
}

// NewRotateWriter creates a new RotateWriter
func NewRotateWriter(filename string, maximumSize uint32, backups int) (w *RotateWriter, err error) {
	if maximumSize == 0 {
		maximumSize = defaultSizeMaximum
	}
	if backups == 0 {
		backups = defaultBackups
	}
	w = &RotateWriter{
		filename:    filename,
		maximumSize: maximumSize,
		backups:     backups,
	}
	err = w.reOpen()
	if err != nil {
		return nil, err
	}
	return w, nil
}

// Close the writer
func (w *RotateWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.close()
}

func (w *RotateWriter) close() error {
	var err error
	w.fpSize = 0
	if w.fp != nil {
		err = w.fp.Sync()
		if err != nil {
			return errors.WithStack(err)
		}
		err = w.fp.Close()
		if err != nil {
			err = errors.WithStack(err)
		}
		w.fp = nil
	}
	return err
}

func (w *RotateWriter) reOpen() (err error) {
	err = w.close()
	if err != nil {
		return err
	}
	w.fp, err = os.OpenFile(w.filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		if w.fp != nil {
			w.fp.Close()
		}
		return err
	}
	info, err := w.fp.Stat()
	if err != nil {
		w.fp.Close()
		return err
	}
	if atomic.AddUint32(&w.fpSize, uint32(info.Size())) >= w.maximumSize {
		err = w.rotate()
		if err != nil {
			w.fp.Close()
			return err
		}
	}
	return nil
}

// Write data, satisfies the io.Writer interface the same time
func (w *RotateWriter) Write(output []byte) (n int, err error) {
	if w.fp == nil {
		return 0, errors.New("writer closed")
	}
	sizeMaximumExceeded := atomic.AddUint32(&w.fpSize, uint32(len(output))) >= w.maximumSize
	n, err = w.fp.Write(output)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	if sizeMaximumExceeded {
		err = w.rotate()
		if err != nil {
			return n, errors.WithStack(err)
		}
	}
	return n, nil
}

// Perform the actual act of rotating and reopening file.
func (w *RotateWriter) rotate() (err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// close
	err = w.close()
	if err != nil {
		return errors.WithStack(err)
	}

	for i := w.backups - 1; i >= 1; i-- {
		backupCurrent := fmt.Sprintf("%s.%d", w.filename, i)
		backupNext := fmt.Sprintf("%s.%d", w.filename, i+1)
		_, err = os.Stat(backupCurrent)
		if err == nil {
			err = os.Rename(backupCurrent, backupNext)
			if err != nil {
				err = errors.WithStack(err)
				return
			}
			continue
		}
		// only not exist error can be ignored
		if !os.IsNotExist(err) {
			err = errors.WithStack(err)
			return
		}
	}
	err = os.Rename(w.filename, fmt.Sprintf("%s.%d", w.filename, 1))
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	return w.reOpen()
}
