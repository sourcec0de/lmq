package lmq

import (
	"fmt"

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
