package lmq

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

func (t *lmdbTopic) choosePartitionForPersist(txn *lmdb.Txn, rotating bool) uint64 {
	id := t.latestPartitionID(txn)

	if rotating && t.partitionID == id {
		id++
		t.updatePartitionMeta(txn, id, t.latestPersistOffset(txn)+1)
	}

	t.partitionID = id
	return t.partitionID
}

func (t *lmdbTopic) latestPartitionID(txn *lmdb.Txn) uint64 {
	cur, err := txn.OpenCursor(t.partitionMetaDB)
	if err != nil {
		log.Fatalln("In lastestPartitionMeta, open cursor failed: ", err)
	}

	idBuf, _, err := cur.Get(nil, nil, lmdb.Last)
	if err != nil {
		log.Fatalln("In lastestPartitionMeta, cursor consume failed: ", err)
	}

	return bytesToUInt64(idBuf)
}

func (t *lmdbTopic) latestPersistOffset(txn *lmdb.Txn) uint64 {
	cur, err := txn.OpenCursor(t.ownerMetaDB)
	if err != nil {
		log.Fatalln("In lastestPartitionMeta, open cursor failed: ", err)
	}

	_, offsetBuf, err := cur.Get(nil, nil, lmdb.Last)
	if err != nil {
		log.Fatalln("In lastestPartitionMeta, cursor consume failed: ", err)
	}

	return bytesToUInt64(offsetBuf)
}

func (t *lmdbTopic) updatePartitionMeta(txn *lmdb.Txn, id, eof uint64) {
	if err := txn.Put(t.partitionMetaDB, uInt64ToBytes(id), uInt64ToBytes(eof), lmdb.Append); err != nil {
		log.Fatalln("updatePartitionMeta failed: ", err)
	}
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
	if err = env.Open(path, lmdb.NoSync|lmdb.NoSubdir, 0664); err != nil {
		err1 := env.Close()
		log.Println("Close env failed: ", err1)
		return err
	}

	t.env = env

	t.waitGroup.Wrap(func() { t.readerCheck() })

	_ = env.Update(func(txn *lmdb.Txn) error {
		partitionName := uInt64ToString(t.partitionID)
		t.partitionDB, err = txn.CreateDBI(partitionName)
		if err != nil {
			log.Fatalln("Open partition failed: ", err)
		}
		return nil
	})

	return nil
}

func (t *lmdbTopic) partitionPath(id uint64) string {
	return fmt.Sprintf("%s/%s.%d", t.dataPath, t.opt.Name, id)
}

func (t *lmdbTopic) persistedOffset(txn *lmdb.Txn) uint64 {
	offsetBuf, err := txn.Get(t.ownerMetaDB, []byte("producer_head"))
	if err != nil {
		log.Fatalln("Persist offset failed: ", err)
	}
	return bytesToUInt64(offsetBuf)
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

func (t *lmdbTopic) updatePersistOffset(txn *lmdb.Txn, offset uint64) {
	if err := txn.Put(t.ownerMetaDB, []byte("producer_head"), uInt64ToBytes(offset), 0); err != nil {
		log.Fatalln("Update persist offset failed!")
	}
}

func (t *lmdbTopic) rotatePersistPartition() {
	err := t.queueEnv.Update(func(txn *lmdb.Txn) error {
		t.closePartition()
		count, err := t.countPartitions(txn)
		if err != nil {
			return err
		}
		if count > t.opt.MaxDataFiles {
			expiredCount := count - t.opt.MaxDataFiles
			t.removeExpiredPartitions(txn, expiredCount)
		}
		partitionID := t.choosePartitionForPersist(txn, true)
		return t.openPersistPartitionDB(partitionID)
	})
	if err != nil {
		log.Fatalln("Rotate persist partition failed")
	}
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

func (t *lmdbTopic) removeExpiredPartitions(txn *lmdb.Txn, expiredCount uint64) {
	cursor, err := txn.OpenCursor(t.partitionMetaDB)
	if err != nil {
		log.Fatalln("Remove expired partitions failed: ", err)
	}

	i := uint64(0)
	var idBuf []byte
	var err1 error
	for idBuf, _, err1 = cursor.Get(nil, nil, lmdb.First); err1 == nil && i < expiredCount; i++ {
		id := bytesToUInt64(idBuf)
		if err := cursor.Del(0); err != nil {
			log.Fatalln("Remove expired partitions failed: ", err)
		}
		partitionPath := t.partitionPath(id)
		if err := os.Remove(partitionPath); err != nil {
			log.Fatalln("Remove expired partitions failed: ", err)
		}
		if err := os.Remove(fmt.Sprintf("%s-lock", partitionPath)); err != nil {
			log.Fatalln("Remove expired partitions failed: ", err)
		}
		idBuf, _, err1 = cursor.Get(nil, nil, lmdb.Next)
	}
}

func (t *lmdbTopic) choosePartitionForConsume(txn *lmdb.Txn, groupID string) uint64 {
	t.partitionID = t.consumePartitionID(txn, groupID, t.partitionID)

	return t.partitionID
}

func (t *lmdbTopic) openConsumePartitionDB(id uint64) error {
	path := t.partitionPath(id)

	env, err := lmdb.NewEnv()
	if err != nil {
		return err
	}
	if err = env.SetMaxDBs(1); err != nil {
		return err
	}
	if err = env.SetMapSize(t.opt.MaxBytesPerFile); err != nil {
		return err
	}
	if err = env.Open(path, lmdb.Readonly|lmdb.NoSync|lmdb.NoSubdir, 0664); err != nil {
		return err
	}

	t.env = env

	t.waitGroup.Wrap(func() { t.readerCheck() })

	err = env.View(func(txn *lmdb.Txn) error {
		t.partitionDB, err = txn.OpenDBI(uInt64ToString(t.partitionID), 0)
		return err
	})
	if err != nil {
		return err
	}

	rtxn, err := env.BeginTxn(nil, lmdb.Readonly)
	if err != nil {
		return err
	}
	cursor, err := rtxn.OpenCursor(t.partitionDB)
	if err != nil {
		return err
	}
	t.cursor = cursor
	rtxn.Reset()
	t.rtxn = rtxn

	return nil
}

func (t *lmdbTopic) consumePartitionID(txn *lmdb.Txn, groupID string, searchFrom uint64) uint64 {
	offset := t.consumeOffset(txn, groupID)

	var partitionID uint64
	var eoffset uint64

	cursor, err := txn.OpenCursor(t.partitionMetaDB)
	if err != nil {
		log.Fatalln("Get consume partition ID faild: ", err)
	}
	k, v, err := cursor.Get(uInt64ToBytes(searchFrom), nil, lmdb.SetRange)
	partitionID = bytesToUInt64(k)
	eoffset = bytesToUInt64(v)

	for err == nil && offset >= eoffset {
		k, v, err = cursor.Get(nil, nil, lmdb.Next)
		if err == nil {
			eoffset = bytesToUInt64(v)
			if offset < eoffset {
				return partitionID
			}
			partitionID = bytesToUInt64(k)
		}
	}
	return partitionID
}

func (t *lmdbTopic) consumeOffset(txn *lmdb.Txn, groupID string) uint64 {
	keyConsumerStr := fmt.Sprintf("%s_%s", "consumer_head", groupID)
	offsetBuf, err := txn.Get(t.ownerMetaDB, []byte(keyConsumerStr))
	if err == nil {
		offset := bytesToUInt64(offsetBuf)
		return offset
	}
	if !lmdb.IsNotFound(err) {
		log.Fatalln("Get consume offset failed: ", err)
	}
	cursor, err := txn.OpenCursor(t.partitionMetaDB)
	if err != nil {
		log.Fatalln("Get consume offset failed: ", err)
	}
	_, offsetBuf, err = cursor.Get(nil, nil, lmdb.First)
	if err != nil {
		log.Fatalln("Get consume offset failed: ", err)
	}
	return bytesToUInt64(offsetBuf)
}

func (t *lmdbTopic) ConsumePartition(groupID string, msgs chan<- *[]byte) (consumed int32, eof bool) {
	_ = t.queueEnv.Update(func(txn *lmdb.Txn) error {
		defer t.rtxn.Reset()
		if err := t.rtxn.Renew(); err != nil {
			log.Println("t.rtxn renew failed")
		}

		pOffset := t.persistedOffset(txn)
		cOffset := t.consumeOffset(txn, groupID)

		if cOffset-pOffset == 1 || pOffset == 0 {
			return nil
		}

		k, v, err := t.cursor.Get(uInt64ToBytes(cOffset), nil, lmdb.SetRange)
		if err == nil {
			var offset uint64
			for ; err == nil && consumed < t.opt.FetchSize; consumed++ {
				offset = bytesToUInt64(k)
				msgs <- &v
				k, v, err = t.cursor.Get(nil, nil, lmdb.Next)
			}
			if offset > 0 {
				t.updateConsumeOffset(txn, groupID, offset+1)
			}
		} else {
			if err != nil {
				if !lmdb.IsNotFound(err) {
					log.Println("Consume partition failed: ", err)
				}
				if cOffset < t.persistedOffset(txn) {
					eof = true
				}
			}
		}
		return nil
	})
	return consumed, eof
}

func (t *lmdbTopic) updateConsumeOffset(txn *lmdb.Txn, groupID string, offset uint64) {
	keyConsumerStr := fmt.Sprintf("%s_%s", "consumer_head", groupID)
	if err := txn.Put(t.ownerMetaDB, []byte(keyConsumerStr), uInt64ToBytes(offset), 0); err != nil {
		log.Panicln("Update consume offset failed: ", err)
	}
}

func (t *lmdbTopic) rotateConsumePartition(groupID string) {
	err := t.queueEnv.Update(func(txn *lmdb.Txn) error {
		t.closePartition()
		partitionID := t.choosePartitionForConsume(txn, groupID)
		return t.openConsumePartitionDB(partitionID)
	})
	if err != nil {
		panic(err)
	}
}

func (t *lmdbTopic) closePartition() {
	t.exitChan <- 1
	if t.cursor != nil {
		t.cursor.Close()
	}
	if t.rtxn != nil {
		t.rtxn.Abort()
	}
	t.env.CloseDBI(t.partitionDB)
	if err := t.env.Close(); err != nil {
		log.Fatalln("Close partition failed: ", err)
	}
}

func (t *lmdbTopic) readerCheck() {
	checkTicker := time.NewTicker(time.Second)
	for {
		select {
		case <-checkTicker.C:
			_, _ = t.env.ReaderCheck()
		case <-t.exitChan:
			goto exit
		}
	}
exit:
	checkTicker.Stop()
}

func (t *lmdbTopic) checkConsumerKeyPrefix(val string) bool {
	return len(val) > len("consumer_head_") && strings.HasPrefix(val, "consumer_head_")
}
