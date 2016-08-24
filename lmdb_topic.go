package lmq

import (
	"fmt"
	"log"
	"runtime"

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
	loading         chan int

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
		loading:         make(chan int, 1),
		env:             nil,
		partitionID:     0,
		partitionDB:     0,
		cursor:          nil,
		rtxn:            nil,
		inFlight:        make(chan int, 1),
	}
}

func (t *lmdbTopic) loadMeta(txn *lmdb.Txn) error {
	t.loading <- 1
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
	err = txn.Put(t.partitionMetaDB, initPartitionID, initOffset, lmdb.NoOverwrite)

	<-t.loading

	return err
}

func (t *lmdbTopic) openPartitionForPersist() {
	err := t.queueEnv.Update(func(txn *lmdb.Txn) error {
		partitionID, err := t.choosePartitionForPersist(txn, false)
		if err != nil {
			return err
		}
		t.partitionID = partitionID
		return t.openPersistPartitionDB(partitionID)
	})
	if err != nil {
		log.Panicf("Open partititon for persist failed: %s", err)
	}
}

func (t *lmdbTopic) persistMessages(msgs []*Message) {
	isFull := false
	err := t.queueEnv.Update(func(txn *lmdb.Txn) error {
		offset, err := t.persistedOffset(txn)
		if err != nil {
			return err
		}
		offset, err = t.persistToPartitionDB(offset, msgs)
		if err != nil {
			return t.updatePersistOffset(txn, offset)
		}
		return err
	})
	if err == nil {
		return
	}
	if lmdb.IsMapFull(err) {
		isFull = true
	} else {
		panic(err)
	}
	if isFull {
		t.rotatePersistPartition()
		t.persistMessages(msgs)
	}
}

func (t *lmdbTopic) openPartitionForConsume(groupID string) {
	err := t.queueEnv.Update(func(txn *lmdb.Txn) error {
		partitionID, err := t.choosePartitionForConsume(txn, groupID)
		if err != nil {
			return err
		}
		return t.openConsumePartitionDB(partitionID)
	})
	if err != nil {
		log.Panicf("Open partititon for persist failed: %s", err)
	}
}

func (t *lmdbTopic) scanMessages(groupID string, msgs chan<- *[]byte) {
	for {
		fetchSize, eof := t.scanPartition(groupID, msgs)
		if (fetchSize == t.opt.fetchSize) || lmdb.IsNotFound(eof) {
			runtime.Gosched()
		}
		if lmdb.IsNotFound(eof) {
			t.rotateScanPartition(groupID)
		}
	}
}

func (t *lmdbTopic) close() {
	t.loading <- 1
	t.inFlight <- 1
	t.exitChan <- 1
	t.waitGroup.Wait()
	close(t.loading)
	close(t.inFlight)
	close(t.exitChan)
}
