package lmq

import (
	"fmt"
	"log"
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
	t.inFlight <- 1
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
	<-t.inFlight
	return offset, err
}

func (t *lmdbTopic) updatePersistOffset(txn *lmdb.Txn, offset uint64) error {
	return txn.Put(t.ownerMetaDB, []byte("producer_head"), uInt64ToBytes(offset+1), 0)
}

func (t *lmdbTopic) rotatePersistPartition() {
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
	partititonID, err := t.consumePartitionID(txn, groupID, t.partitionID)
	if err != nil {
		return 0, nil
	}
	t.partitionID = partititonID
	return partititonID, nil
}

func (t *lmdbTopic) openConsumePartitionDB(id uint64) error {
	path := t.partitionPath(id)

	env, err := lmdb.NewEnv()
	if err != nil {
		return err
	}
	t.env = env
	if err = env.SetMaxDBs(1); err != nil {
		return err
	}
	if err = env.SetMapSize(t.opt.MaxBytesPerFile); err != nil {
		return err
	}
	if err = env.Open(path, lmdb.Readonly|lmdb.NoSync|lmdb.NoSubdir, 0644); err != nil {
		return err
	}
	if _, err = env.ReaderCheck(); err != nil {
		return err
	}
	err = env.View(func(txn *lmdb.Txn) error {
		t.partitionDB, err = txn.CreateDBI(uInt64ToString(t.partitionID))
		return err
	})
	if err != nil {
		return err
	}
	rtxn, err := env.BeginTxn(nil, lmdb.Readonly)
	if err != nil {

		return err
	}
	t.rtxn = rtxn
	cursor, err := rtxn.OpenCursor(t.partitionDB)
	if err != nil {

		return err
	}
	t.cursor = cursor
	rtxn.Reset()

	return rtxn.Renew()
}

func (t *lmdbTopic) consumePartitionID(txn *lmdb.Txn, groupID string, searchFrom uint64) (uint64, error) {
	offset, err := t.consumeOffset(txn, groupID)
	if err != nil {
		return 0, err
	}

	var partitionID uint64
	var eoffset uint64

	cursor, err := txn.OpenCursor(t.partitionMetaDB)
	if err != nil {
		return 0, err
	}
	k, v, err := cursor.Get(uInt64ToBytes(searchFrom), nil, lmdb.SetRange)
	partitionID = bytesToUInt64(k)
	eoffset = bytesToUInt64(v)
	for err == nil && offset >= eoffset {
		k, v, err = cursor.Get(nil, nil, lmdb.Next)
		if err == nil {
			eoffset = bytesToUInt64(v)
			if offset < eoffset {
				return partitionID, nil
			}
			partitionID = bytesToUInt64(k)
		}
	}

	return partitionID, nil
}

func (t *lmdbTopic) consumeOffset(txn *lmdb.Txn, groupID string) (uint64, error) {
	return 0, nil
}

func (t *lmdbTopic) scanPartition(groupID string, msgs chan<- *[]byte) (int32, error) {
	var scanned int32
	err := t.queueEnv.Update(func(txn *lmdb.Txn) error {
		pOffset, err := t.persistedOffset(txn)
		if err != nil {
			return err
		}
		cOffset, err := t.consumeOffset(txn, groupID)
		if err != nil {
			return err
		}
		if pOffset-cOffset == 1 || pOffset == 0 {
			return nil
		}
		k, v, err := t.cursor.Get(uInt64ToBytes(cOffset), nil, lmdb.SetRange)
		offset := bytesToUInt64(k)
		for ; err == nil && scanned < t.opt.fetchSize; scanned++ {
			msgs <- &v
			k, v, err = t.cursor.Get(nil, nil, lmdb.Next)
			if err != nil && lmdb.IsNotFound(err) {
				break
			}
			offset = bytesToUInt64(k)
		}
		if offset > 0 {
			t.updateConsumeOffset(txn, groupID, offset+1)
		}
		if lmdb.IsNotFound(err) {
			return nil
		}
		return err
	})
	if err != nil {
		log.Fatalln("Scan partition failed: ", err)
	}
	return scanned, err
}

func (t *lmdbTopic) updateConsumeOffset(txn *lmdb.Txn, groupID string, offset uint64) {
	keyConsumerStr := fmt.Sprintf("%s_%s", "consumer_head", groupID)
	if err := txn.Put(t.ownerMetaDB, []byte(keyConsumerStr), uInt64ToBytes(offset), 0); err != nil {
		log.Panicln("Update consume offset failed: ", err)
	}
}

func (t *lmdbTopic) rotateScanPartition(groupID string) {
	err := t.queueEnv.Update(func(txn *lmdb.Txn) error {
		if err := t.closePartition(); err != nil {
			return err
		}
		partitionID, err := t.choosePartitionForConsume(txn, groupID)
		if err != nil {
			return err
		}
		return t.openConsumePartitionDB(partitionID)
	})
	if err != nil {
		panic(err)
	}
}

func (t *lmdbTopic) closePartition() error {
	if t.cursor != nil {
		t.cursor.Close()
	}
	if t.rtxn != nil {
		t.rtxn.Abort()
	}
	t.env.CloseDBI(t.partitionDB)
	return t.env.Close()
}
