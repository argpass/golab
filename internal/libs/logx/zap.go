package logx

import (
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

type graceEncoder struct {
	zapcore.Encoder
}

func NewGraceEncoder(enc zapcore.Encoder) zapcore.Encoder {
	return graceEncoder{
		Encoder: enc,
	}
}
func (enc graceEncoder) EncodeEntry(zapcore.Entry, []zapcore.Field) (*buffer.Buffer, error) {
	return nil, nil
}
