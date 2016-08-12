package lmq

import (
	"fmt"
	"sync"
	"time"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

type topic struct {
	opt TopicOption

	ownerMetaDB     lmdb.DBI
	partitionMetaDB lmdb.DBI
}

func newTopic(name string, opt *Options) *topic {
	return &topic{
		opt:             opt.Topics[name],
		ownerMetaDB:     0,
		partitionMetaDB: 0,
	}
}

func (t *topic) loadMeta(txn *lmdb.Txn) error {
	ownerMetaDBName := fmt.Sprintf("%s-%s", t.opt.Name, "ownerMeta")
	ownerMetaDB, err := txn.CreateDBI(ownerMetaDBName)
	if err != nil {
		return err
	}
	t.ownerMetaDB = ownerMetaDB
	initOffset := uInt64ToBytes(0)
	err = txn.Put(ownerMetaDB, []byte("producer_head"), initOffset, lmdb.NoOverwrite)
	if err != nil {
		if err, ok := err.(*lmdb.OpError); ok {
			if err.Errno == lmdb.KeyExist {
				return nil
			}
			return err
		}
	}
	partitionMetaDBName := fmt.Sprintf("%s-%s", t.opt.Name, "partitionMeta")
	partitionMetaDB, err := txn.CreateDBI(partitionMetaDBName)
	if err != nil {
		return err
	}
	t.partitionMetaDB = partitionMetaDB
	initPartitionID := initOffset
	return txn.Put(t.partitionMetaDB, initPartitionID, initOffset, lmdb.NoOverwrite)
}

type LmdbBackendStorage struct {
	env   *lmdb.Env
	topic map[string]*topic
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
	lbs := &LmdbBackendStorage{
		env: env,
	}
	lbs.waitGroup.Wrap(func() { lbs.readerCheck() })
	return lbs, nil
}

func (lbs *LmdbBackendStorage) LoadTopicMeta(topic string) error {
	lbs.RLock()
	_, ok := lbs.topic[topic]
	lbs.RUnlock()
	if ok {
		return nil
	}

	lbs.Lock()

	_, ok = lbs.topic[topic]
	if ok {
		return nil
	}

	t := newTopic(topic, lbs.opt)
	err := lbs.env.Update(func(txn *lmdb.Txn) error {
		return t.loadMeta(txn)
	})
	lbs.topic[topic] = t
	lbs.Unlock()

	return err
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
