package backend

import (
	"bytes"
	"time"
	"math/rand"
	"github.com/dbjtech/golab/pending/wrappers"
	"github.com/dbjtech/golab/eventer/helper"
)

const (
	EMPTY_SESSION_ID = "00000000"
)

type Session map[string]interface{}

func (s *Session) GetSessionId() string {
	s["session_id"]
}

type SessionStorage struct {
	redis *wrappers.MyRedis
}

func (storage *SessionStorage) GetSession(sessionId string) (Session, error){
	var v map[string]interface{}
	err := storage.redis.GetValue(helper.R_SessionKey(sessionId), &v)
	if err != nil {
		return nil, err
	}
	return Session(v), nil
}

func GenSessionID() string {
	var bf *bytes.Buffer = bytes.NewBufferString("")
	var seed = []string{
		"0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
		"A", "B", "C", "E", "D", "F",
	}
	rand.Seed(time.Now().Unix())
	for i := 0; i < len(EMPTY_SESSION_ID); i++ {
		gotIndex := rand.Intn(len(seed))
		bf.WriteString(seed[gotIndex])
	}
	if bf.String() == EMPTY_SESSION_ID {
		return GenSessionID()
	}
	return bf.String()
}
