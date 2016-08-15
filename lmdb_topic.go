package lmq

import (
	"fmt"
	"log"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

type lmdbPartitionMeta struct {
	id     uint64
	offset uint64
}

type lmdbTopic struct {
	opt      TopicOption
	dataPath string

	ownerMetaDB     lmdb.DBI
	partitionMetaDB lmdb.DBI
	partitionID     uint64

	env         *lmdb.Env
	partitionDB lmdb.DBI
}

func newLmdbTopic(name string, opt *Options) *lmdbTopic {
	return &lmdbTopic{
		opt:             opt.Topics[name],
		dataPath:        opt.DataPath,
		ownerMetaDB:     0,
		partitionMetaDB: 0,
		partitionID:     0,
		env:             nil,
		partitionDB:     0,
	}
}

func (t *lmdbTopic) loadMeta(txn *lmdb.Txn) error {
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

func (t *lmdbTopic) openPartitionForPersist() {
	err := t.env.Update(func(txn *lmdb.Txn) error {
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

func (t *lmdbTopic) persistedOffset(txn *lmdb.Txn) (uint64, error) {
	offsetBuf, err := txn.Get(t.ownerMetaDB, []byte("producer_head"))
	if err != nil {
		return 0, err
	}
	return bytesToUInt64(offsetBuf), err
}
