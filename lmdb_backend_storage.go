package lmq

import (
	"fmt"
	"log"
	"time"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

type lmdbBackendStorage struct {
	env *lmdb.Env

	opt *Options

	exitChan  chan struct{}
	waitGroup WaitGroupWrapper
}

// NewLmdbBackendStorage creates a new BackendStorage(using lmdb) using the given option.
func NewLmdbBackendStorage(opt *Options) (BackendStorage, error) {
	env, err := lmdb.NewEnv()
	if err != nil {
		return nil, err
	}
	if err = env.SetMapSize(int64(opt.MaxTopicCount * 1024 * 1024)); err != nil {
		return nil, err
	}
	if err = env.SetMaxDBs(opt.MaxTopicCount * 2); err != nil {
		return nil, err
	}
	envPath := fmt.Sprintf("%s/%s", opt.DataPath, "__meta__")
	if err = env.Open(envPath, lmdb.NoSync|lmdb.NoSubdir, 0644); err != nil {
		return nil, err
	}
	lbs := &lmdbBackendStorage{
		env:      env,
		opt:      opt,
		exitChan: make(chan struct{}),
	}
	lbs.waitGroup.Wrap(func() { lbs.readerCheck() })
	return lbs, nil
}

func (lbs *lmdbBackendStorage) OpenTopic(topic, groupID string, flag int) Topic {
	t := newLmdbTopic(topic, lbs.env, lbs.opt)

	loadTopicMeta := func(txn *lmdb.Txn) error {
		t.loadMeta(txn)
		return nil
	}
	_ = lbs.env.Update(loadTopicMeta)

	switch flag {
	case 0:
		t.openPartitionForPersist()
	case 1:
		t.openPartitionForConsume(groupID)
	case 2:
		break
	default:
		log.Fatalf("Open topic faild: unvaild %d flag", flag)
	}

	return t
}

func (lbs *lmdbBackendStorage) PersistMessages(topic Topic, msgs []*Message) {
	topic.(*lmdbTopic).persistMessages(msgs)
}

func (lbs *lmdbBackendStorage) ScanMessages(topic Topic, groupID string, msgs chan<- *[]byte) {
	topic.(*lmdbTopic).scanMessages(groupID, msgs)
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

func (lbs *lmdbBackendStorage) Stat(topic Topic) *TopicStat {
	return topic.(*lmdbTopic).stat()
}

func (lbs *lmdbBackendStorage) CloseTopic(topic Topic) {
	topic.(*lmdbTopic).close()
}

func (lbs *lmdbBackendStorage) Close() {
	close(lbs.exitChan)
	if err := lbs.env.Close(); err != nil {
		log.Fatalln("Close queue failed: ", err)
	}
}
