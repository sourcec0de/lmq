package lmq

import (
	"fmt"
	"log"
	"runtime"
	"strings"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

type lmdbPartitionMeta struct {
	id     uint64
	offset uint64
}

type lmdbTopic struct {
	opt      TopicOption
	dataPath string

	queueEnv *lmdb.Env

	ownerMetaDB     lmdb.DBI
	partitionMetaDB lmdb.DBI

	env         *lmdb.Env
	partitionID uint64
	partitionDB lmdb.DBI
	cursor      *lmdb.Cursor
	rtxn        *lmdb.Txn
	inFlight    chan int

	exitChan  chan int
	waitGroup WaitGroupWrapper
}

func newLmdbTopic(name string, queueEvn *lmdb.Env, opt *Options) *lmdbTopic {
	return &lmdbTopic{
		opt:             opt.Topics[name],
		dataPath:        opt.DataPath,
		queueEnv:        queueEvn,
		ownerMetaDB:     0,
		partitionMetaDB: 0,
		env:             nil,
		partitionID:     0,
		partitionDB:     0,
		cursor:          nil,
		rtxn:            nil,
		inFlight:        make(chan int, 1),
		exitChan:        make(chan int, 1),
	}
}

func (t *lmdbTopic) loadMeta(txn *lmdb.Txn) {
	partitionMetaInited := false
	ownerMetaDBName := fmt.Sprintf("%s-%s", t.opt.Name, "ownerMeta")
	ownerMetaDB, err := txn.CreateDBI(ownerMetaDBName)
	if err != nil {
		log.Fatalln("Load topic Meta failed: ", err)
	}
	t.ownerMetaDB = ownerMetaDB
	initOffset := uInt64ToBytes(0)
	err = txn.Put(ownerMetaDB, []byte("producer_head"), initOffset, lmdb.NoOverwrite)
	if err != nil {
		if err, ok := err.(*lmdb.OpError); ok {
			if err.Errno != lmdb.KeyExist {
				log.Fatalln("Load topic Meta failed: ", err)
			}
			partitionMetaInited = true
		}
	}
	partitionMetaDBName := fmt.Sprintf("%s-%s", t.opt.Name, "partitionMeta")
	partitionMetaDB, err := txn.CreateDBI(partitionMetaDBName)
	if err != nil {
		log.Fatalln("Load topic Meta failed: ", err)
	}
	t.partitionMetaDB = partitionMetaDB
	if !partitionMetaInited {
		initPartitionID := initOffset
		if err = txn.Put(t.partitionMetaDB, initPartitionID, initOffset, lmdb.NoOverwrite); err != nil {
			log.Fatalln("Load topic Meta failed: ", err)
		}
	}
}

func (t *lmdbTopic) OpenPartitionForPersist() {
	err := t.queueEnv.Update(func(txn *lmdb.Txn) error {
		partitionID := t.choosePartitionForPersist(txn, false)
		t.partitionID = partitionID
		return t.openPersistPartitionDB(partitionID)
	})
	if err != nil {
		log.Panicf("Open partititon for persist failed: %s, gid: %d", err, GoID())
	}
}

func (t *lmdbTopic) PersistMessages(msgs []*Message) {
	isFull := false
	_ = t.queueEnv.Update(func(txn *lmdb.Txn) error {
		offset := t.persistedOffset(txn)
		offset, err := t.persistToPartitionDB(offset, msgs)
		if lmdb.IsMapFull(err) {
			isFull = true
		}
		if !isFull {
			t.updatePersistOffset(txn, offset)
		}
		return err
	})
	if isFull {
		t.rotatePersistPartition()
		t.PersistMessages(msgs)
	}
}

func (t *lmdbTopic) OpenPartitionForConsume(groupID string) {
	err := t.queueEnv.Update(func(txn *lmdb.Txn) error {
		partitionID := t.choosePartitionForConsume(txn, groupID)
		return t.openConsumePartitionDB(partitionID)
	})
	if err != nil {
		log.Panicf("Open partititon for consume failed: %s", err)
	}
}

func (t *lmdbTopic) ConsumeMessages(groupID string, msgs chan<- *[]byte) {
	fetchSize, eof := t.ConsumePartition(groupID, msgs)
	if (fetchSize == t.opt.FetchSize) || eof {
		runtime.Gosched()
	}
	if eof {
		t.rotateConsumePartition(groupID)
	}
}

func (t *lmdbTopic) close() {
	t.inFlight <- 1
	t.exitChan <- 1
	t.waitGroup.Wait()
	close(t.inFlight)
	close(t.exitChan)
}

type TopicStat struct {
	PersistedOffset uint64
	ConsumeOffset   map[string]uint64
}

func (t *lmdbTopic) stat() *TopicStat {
	ts := &TopicStat{}
	_ = t.queueEnv.Update(func(txn *lmdb.Txn) error {
		ts.PersistedOffset = t.persistedOffset(txn)
		cursor, err := txn.OpenCursor(t.ownerMetaDB)
		if err != nil {
			return nil
		}
		consumerBuf, offsetBuf, err := cursor.Get(nil, nil, lmdb.First)
		if err != nil {
			return nil
		}
		for err == nil && t.checkConsumerKeyPrefix(string(consumerBuf)) {
			consumer := string(consumerBuf)
			name := strings.TrimPrefix(consumer, "consumer_head_")
			ts.ConsumeOffset[name] = bytesToUInt64(offsetBuf)
			consumerBuf, offsetBuf, err = cursor.Get(nil, nil, lmdb.Next)
		}
		return nil
	})
	return ts
}
