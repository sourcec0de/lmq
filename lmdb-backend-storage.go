package lmq

import (
	"fmt"
	"time"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

type LmdbBackendStorage struct {
	env       *lmdb.Env
	exitChan  chan int
	waitGroup WaitGroupWrapper
}

func NewLmdbBackendStorage(opt *Options) (BackendStorage, error) {
	env, err := lmdb.NewEnv()
	if err != nil {
		return nil, err
	}
	if err = env.SetMapSize(int64(opt.MaxTopicCount * 1024 * 1024)); err != nil {
		return nil, err
	}
	if err = env.SetMaxDBs(opt.MaxTopicCount); err != nil {
		return nil, err
	}
	envPath := fmt.Sprintf("%s%s", opt.DataPath, "__meta__")
	if err = env.Open(envPath, lmdb.NoSync|lmdb.NoSubdir, 0644); err != nil {
		return nil, err
	}
	if _, err := env.ReaderCheck(); err != nil {
		return nil, err
	}
	lbs := &LmdbBackendStorage{
		env: env,
	}
	lbs.waitGroup.Wrap(func() { lbs.readerCheck() })
	return lbs, nil
}

func (lbs *LmdbBackendStorage) InitTopicMeta(topic string) {

}

func (lbs *LmdbBackendStorage) InitPartitionMeta(topic, partition string) {

}

func (lbs *LmdbBackendStorage) PersistMessages(topic string, msgs []*Message) {

}

func (lbs *LmdbBackendStorage) readerCheck() {
	checkTicker := time.NewTicker(time.Second)
	for {
		select {
		case <-checkTicker.C:
			_, _ = lbs.env.ReaderCheck()
		case <-lbs.exitChan:
			goto exit
		}
	}
exit:
	checkTicker.Stop()
}
