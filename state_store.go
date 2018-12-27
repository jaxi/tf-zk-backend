package main

import (
	"errors"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/sirupsen/logrus"
)

// StateStore manages terraform state using zookeeper
type StateStore struct {
	Zks    []string
	logger *logrus.Logger
}

var (
	ErrConn     = errors.New("zk-terraform-backend: cannot connect to zk")
	ErrNotExist = errors.New("zk-terraform-backend: cannot found znode")
	ErrCreate   = errors.New("zk-terraform-backend: cannot create znode")
	ErrUpdate   = errors.New("zk-terraform-backend: cannot update znode")
	ErrDelete   = errors.New("zk-terraform-backend: cannot delete znode")
	ErrRead     = errors.New("zk-terraform-backend: cannot read znode")
	ErrWrite    = errors.New("zk-terraform-backend: cannot write to znode")
)

func (store *StateStore) Get(name string) ([]byte, error) {
	znode := "/" + name

	ctxLog := store.logger.WithFields(logrus.Fields{
		"znode": znode,
	})
	ctxLog.Debug("Get znode state")

	conn, _, err := zk.Connect(store.Zks, time.Second)
	if err != nil {
		ctxLog.WithFields(logrus.Fields{
			"reason": err.Error(),
		}).Error("Cannot connect to zk")
		return []byte{}, ErrConn
	}
	defer conn.Close()

	data, _, err := conn.Get(znode)
	if err != nil {
		ctxLog = ctxLog.WithFields(logrus.Fields{
			"reason": err.Error(),
		})
	}
	switch err {
	case nil:
		ctxLog.Infof("Terraform state retrieved")
		return data, nil
	case zk.ErrNoNode:
		ctxLog.Error("Terraform state does not exist")
		return data, ErrNotExist
	default:
		ctxLog.Error("Terraform state cannot be retrieved")
		return data, ErrRead
	}
}

func (store *StateStore) Update(name string, state []byte) error {
	znode := "/" + name

	ctxLog := store.logger.WithFields(logrus.Fields{
		"znode": znode,
	})
	ctxLog.Debug("Update znode state")

	conn, _, err := zk.Connect(store.Zks, time.Second)
	if err != nil {
		ctxLog.WithFields(logrus.Fields{
			"reason": err.Error(),
		}).Error("Cannot connect to zk")
		return ErrConn
	}
	defer conn.Close()

	exists, stat, err := conn.Exists(znode)
	if err != nil {
		ctxLog.WithFields(logrus.Fields{
			"reason": err.Error(),
		}).Error("Cannot check znode's existance")
		return ErrRead
	}

	if !exists {
		_, err = conn.Create(znode, state, int32(0), zk.WorldACL(zk.PermAll))
		if err != nil {
			ctxLog.WithFields(logrus.Fields{
				"reason": err.Error(),
			}).Error("Cannot create znode")
			return ErrCreate
		}
		ctxLog.Info("Terraform state created")
		return nil
	}

	ctxLog = ctxLog.WithFields(logrus.Fields{
		"znode":        znode,
		"stat_version": stat.Version,
	})

	ctxLog.Info("Update terraform state")

	_, err = conn.Set(znode, state, stat.Version)
	if err != nil {
		ctxLog.WithFields(logrus.Fields{
			"reason": err.Error(),
		}).Error("Cannot update znode")
		return ErrUpdate
	}
	ctxLog.Info("Terraform state updated")
	return nil
}

func (store *StateStore) Delete(name string) error {
	znode := "/" + name

	ctxLog := store.logger.WithFields(logrus.Fields{
		"znode": znode,
	})
	ctxLog.Debug("Delete znode state")

	conn, _, err := zk.Connect(store.Zks, time.Second)
	if err != nil {
		ctxLog.WithFields(logrus.Fields{
			"reason": err.Error(),
		}).Error("Cannot connect to zk")
		return ErrConn
	}
	defer conn.Close()

	exists, stat, err := conn.Exists(znode)
	if err != nil {
		ctxLog.WithFields(logrus.Fields{
			"reason": err.Error(),
		}).Error("Cannot check znode's existance")
		return ErrRead
	}

	if !exists {
		ctxLog.Error("Terraform state does not exist")
		return ErrNotExist
	}

	ctxLog = ctxLog.WithFields(logrus.Fields{
		"znode":        znode,
		"stat_version": stat.Version,
	})

	ctxLog.Info("Delete terraform state")

	if err := conn.Delete(znode, stat.Version); err != nil {
		ctxLog.WithFields(logrus.Fields{
			"reason": err.Error(),
		}).Error("Cannot delete znode")
		return ErrDelete
	}

	ctxLog.Info("Terraform state deleted")
	return nil
}

func (store *StateStore) Lock(name string, lockinfo []byte) (alreadyLocked bool, newlockinfo []byte, err error) {
	znode := "/lock-" + name

	ctxLog := store.logger.WithFields(logrus.Fields{
		"znode": znode,
	})
	ctxLog.Debug("Lock znode state")

	conn, _, err := zk.Connect(store.Zks, time.Second)
	if err != nil {
		ctxLog.WithFields(logrus.Fields{
			"reason": err.Error(),
		}).Error("Cannot connect to zk")
		return false, []byte{}, ErrConn
	}
	defer conn.Close()

	exists, _, err := conn.Exists(znode)
	if err != nil {
		ctxLog.WithFields(logrus.Fields{
			"reason": err.Error(),
		}).Error("Cannot check znode's existance")
		return false, []byte{}, ErrRead
	}

	if exists {
		existingLock, _, err := conn.Get(znode)
		if err != nil {
			ctxLog.Error("Terraform lock state cannot be retrieved")
			return false, []byte{}, ErrRead
		}
		ctxLog.Info("Terraform lock exists")
		return true, existingLock, nil
	}

	_, err = conn.Create(znode, lockinfo, int32(0), zk.WorldACL(zk.PermAll))
	if err != nil {
		ctxLog.WithFields(logrus.Fields{
			"reason": err.Error(),
		}).Error("Cannot create znode")
		return false, []byte{}, ErrCreate
	}
	ctxLog.Info("Terraform lock created")
	return false, lockinfo, nil
}

func (store *StateStore) Unlock(name string) error {
	znode := "/lock-" + name

	ctxLog := store.logger.WithFields(logrus.Fields{
		"znode": znode,
	})
	ctxLog.Debug("Unlock terraform state")

	conn, _, err := zk.Connect(store.Zks, time.Second)
	if err != nil {
		ctxLog.WithFields(logrus.Fields{
			"reason": err.Error(),
		}).Error("Cannot connect to zk")
		return ErrConn
	}
	defer conn.Close()

	exists, stat, err := conn.Exists(znode)
	if err != nil {
		ctxLog.WithFields(logrus.Fields{
			"reason": err.Error(),
		}).Error("Cannot check znode's existance")
		return ErrRead
	}

	if !exists {
		ctxLog.Error("Terraform lockinfo does not exist")
		return ErrNotExist
	}

	ctxLog = ctxLog.WithFields(logrus.Fields{
		"znode":        znode,
		"stat_version": stat.Version,
	})

	ctxLog.Info("Delete terraform lockinfo")

	if err := conn.Delete(znode, stat.Version); err != nil {
		ctxLog.WithFields(logrus.Fields{
			"reason": err.Error(),
		}).Error("Cannot delete znode")
		return ErrDelete
	}

	ctxLog.Info("Terraform state unlocked")
	return nil
}
