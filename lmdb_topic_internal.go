package lmq

import (
	"fmt"
	"os"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

func (t *lmdbTopic) choosePartitionForPersist(txn *lmdb.Txn, rotating bool) (uint64, error) {
	pm, err := t.lastestPartitionMeta(txn)
	if err != nil {
		return 0, err
	}

	if rotating && t.partitionID == pm.id {
		pm.id++
		pm.offset++
		err := t.updatePartitionMeta(txn, pm)
		if err != nil {
			return 0, err
		}
	}

	t.partitionID = pm.id
	return t.partitionID, nil
}

func (t *lmdbTopic) lastestPartitionMeta(txn *lmdb.Txn) (*lmdbPartitionMeta, error) {
	cur, err := txn.OpenCursor(t.partitionMetaDB)
	if err != nil {
		return nil, err
	}

	idBuf, offsetBuf, err := cur.Get(nil, nil, lmdb.Last)
	if err != nil {
		return nil, err
	}

	pm := &lmdbPartitionMeta{
		id:     bytesToUInt64(idBuf),
		offset: bytesToUInt64(offsetBuf),
	}
	return pm, nil
}

func (t *lmdbTopic) updatePartitionMeta(txn *lmdb.Txn, pm *lmdbPartitionMeta) error {
	return txn.Put(t.partitionMetaDB, uInt64ToBytes(pm.id), uInt64ToBytes(pm.offset), lmdb.Append)
}

func (t *lmdbTopic) openPersistPartitionDB(id uint64) error {
	path := t.partitionPath(id)

	env, err := lmdb.NewEnv()
	if err != nil {
		return err
	}
	if err = env.SetMapSize(t.opt.MaxBytesPerFile); err != nil {
		return err
	}
	if err = env.SetMaxDBs(1); err != nil {
		return err
	}
	if err = env.Open(path, lmdb.NoSync|lmdb.NoSubdir, 0644); err != nil {
		return err
	}
	err = env.Update(func(txn *lmdb.Txn) error {
		partitionName := uInt64ToString(t.partitionID)
		t.partitionDB, err = txn.CreateDBI(partitionName)
		return err
	})
	if err != nil {
		return err
	}
	t.env = env
	return nil
}

func (t *lmdbTopic) partitionPath(id uint64) string {
	return fmt.Sprintf("%s/%s.%d", t.dataPath, t.opt.Name, id)
}

func (t *lmdbTopic) persistedOffset(txn *lmdb.Txn) (uint64, error) {
	offsetBuf, err := txn.Get(t.ownerMetaDB, []byte("producer_head"))
	if err != nil {
		return 0, err
	}
	return bytesToUInt64(offsetBuf), err
}

func (t *lmdbTopic) persistToPartitionDB(offset uint64, msgs []*Message) (uint64, error) {
	err := t.env.Update(func(txn *lmdb.Txn) error {
		for _, v := range msgs {
			offset++
			k := uInt64ToBytes(offset)
			if err := txn.Put(t.partitionDB, k, v.Body, lmdb.Append); err != nil {
				return err
			}
		}
		return nil
	})
	return offset, err
}

func (t *lmdbTopic) updatePersistOffset(txn *lmdb.Txn, offset uint64) error {
	return txn.Put(t.ownerMetaDB, []byte("producer_head"), uInt64ToBytes(offset+1), 0)
}

func (t *lmdbTopic) persistRotate() {
	err := t.queueEnv.Update(func(txn *lmdb.Txn) error {
		if err := t.closeCurrentPersistPartition(); err != nil {
			return err
		}
		count, err := t.countPartitions(txn)
		if err != nil {
			return err
		}
		if count > t.opt.MaxDataFiles {
			expiredCount := count - t.opt.MaxDataFiles
			if err := t.removeExpiredPartitions(txn, expiredCount); err != nil {
				return err
			}
		}
		partitionID, err := t.choosePartitionForPersist(txn, true)
		if err != nil {
			return err
		}
		return t.openPersistPartitionDB(partitionID)
	})
	if err != nil {
		panic(err)
	}
}

func (t *lmdbTopic) closeCurrentPersistPartition() error {
	t.env.CloseDBI(t.partitionDB)
	return t.env.Close()
}

func (t *lmdbTopic) countPartitions(txn *lmdb.Txn) (uint64, error) {
	cursor, err := txn.OpenCursor(t.partitionMetaDB)
	if err != nil {
		return 0, err
	}
	beginIDBbuf, _, err := cursor.Get(nil, nil, lmdb.First)
	if err != nil {
		return 0, err
	}
	endIDBbuf, _, err := cursor.Get(nil, nil, lmdb.Last)
	if err != nil {
		return 0, err
	}
	return bytesToUInt64(endIDBbuf) - bytesToUInt64(beginIDBbuf) + 1, nil
}

func (t *lmdbTopic) removeExpiredPartitions(txn *lmdb.Txn, expiredCount uint64) error {
	cursor, err := txn.OpenCursor(t.partitionMetaDB)
	if err != nil {
		return err
	}

	i := uint64(0)
	var idBuf []byte
	var err1 error
	for idBuf, _, err1 = cursor.Get(nil, nil, lmdb.First); err1 == nil && i < expiredCount; i++ {
		id := bytesToUInt64(idBuf)
		if err := cursor.Del(0); err != nil {
			return err
		}
		partitionPath := t.partitionPath(id)
		if err := os.Remove(partitionPath); err != nil {
			return err
		}
		if err := os.Remove(fmt.Sprintf("%s-lock", partitionPath)); err != nil {
			return err
		}
		idBuf, _, err1 = cursor.Get(nil, nil, lmdb.Next)
	}
	return err1
}

func (t *lmdbTopic) choosePartitionForConsume(txn *lmdb.Txn, groupID string) (uint64, error) {
	return 0, nil
}

func (t *lmdbTopic) openConsumPartitionDB(id uint64) error {
	return nil
}
