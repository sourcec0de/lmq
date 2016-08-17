package lmq

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

type lmdbBackendStorage struct {
	env   *lmdb.Env
	topic map[string]*lmdbTopic
	sync.RWMutex

	opt *Options

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
	lbs := &lmdbBackendStorage{
		env: env,
	}
	lbs.waitGroup.Wrap(func() { lbs.readerCheck() })
	return lbs, nil
}

func (lbs *lmdbBackendStorage) OpenTopic(topic, groupID string, flag int) (Topic, error) {
	lbs.RLock()
	t, ok := lbs.topic[topic]
	lbs.RUnlock()
	if ok {
		return t, nil
	}

	lbs.Lock()

	t, ok = lbs.topic[topic]
	if ok {
		return t, nil
	}

	t = newLmdbTopic(topic, lbs.env, lbs.opt)
	err := lbs.env.Update(func(txn *lmdb.Txn) error {
		return t.loadMeta(txn)
	})
	lbs.topic[topic] = t
	lbs.Unlock()

	switch flag {
	case 0:
		t.openPartitionForPersist()
	case 1:
		t.openPartitionForConsume(groupID)
	default:
		log.Fatalf("Open topic faild: unvaild %d flag", flag)
	}

	return t, err
}

func (lbs *lmdbBackendStorage) PersistMessages(topic Topic, msgs []*Message) {
	topic.(*lmdbTopic).persistMessages(msgs)
}

func (lbs *lmdbBackendStorage) readerCheck() {
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
