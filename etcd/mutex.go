package etcd

import (
	"context"
	"errors"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"time"
)

var MutexGrabFAIL = errors.New("抢锁失败")

// EMutex
// @Description: etcd锁的接口方法
type EMutex interface {
	Lock(v string) error

	UnLock() error
}

/*
EtcdMutex

	@Description: etcd锁
*/
type EtcdMutex struct {
	ttl     int64 // 租约
	client  *clientv3.Client
	Key     string
	cancel  context.CancelFunc // 关闭续租的func
	lease   clientv3.Lease
	leaseId clientv3.LeaseID
	txn     clientv3.Txn
	lock    bool
}

/*
NewMutex

	@Description: 创建etcd锁
	@param name: 锁名称
	@param ttl: 锁的租约
	@return *EtcdMutex: etcd 锁对象
*/
func NewMutex(name string, ttl int64) EMutex {
	if !etcdCLI.invited {
		return nil
	}
	if etcdCLI.Err() != nil {
		return nil
	}
	mtx := &EtcdMutex{
		ttl:    ttl,
		Key:    name,
		client: etcdCLI.Client(),
	}
	_ = mtx.Init()
	return mtx
}

func (e *EtcdMutex) Init() error {
	var ctx context.Context
	e.txn = clientv3.NewKV(e.client).Txn(context.TODO())
	e.lease = clientv3.NewLease(e.client)
	leaseResp, err := e.lease.Grant(context.TODO(), e.ttl)
	if err != nil {
		return err
	}
	ctx, e.cancel = context.WithCancel(context.TODO())
	e.leaseId = leaseResp.ID + 1
	_, err = e.lease.KeepAlive(ctx, e.leaseId)
	return err
}

/*
Lock

	@Description: 抢锁
	@return error
*/
func (e *EtcdMutex) Lock(v string) error {
	var (
		ctx         context.Context
		keepResChan <-chan *clientv3.LeaseKeepAliveResponse
	)
	// 创建上下文
	ctx, e.cancel = context.WithCancel(context.TODO())

	// 创建事务txn
	e.txn = clientv3.NewKV(e.client).Txn(context.TODO())

	// 创建续租
	e.lease = clientv3.NewLease(e.client)
	leaseResp, err := e.lease.Grant(context.TODO(), e.ttl)
	if err != nil {
		goto FAIL
	}
	// 续租id
	e.leaseId = leaseResp.ID
	// 保持续租
	keepResChan, err = e.lease.KeepAlive(ctx, e.leaseId)
	if keepResChan, err = e.lease.KeepAlive(ctx, e.leaseId); err != nil {
		goto FAIL
	}
	// 每隔1s续租
	go func() {
		var keepRes *clientv3.LeaseKeepAliveResponse
		for {
			select {
			case keepRes = <-keepResChan:
				if keepRes == nil {
					goto END
				}
			}
			time.Sleep(1 * time.Second)
		}
	END:
	}()
	e.lock = true

	// 提交事务链抢锁
	e.txn.If(clientv3.Compare(clientv3.CreateRevision(e.Key), "=", 0)).Then(clientv3.OpPut(e.Key, v, clientv3.WithLease(e.leaseId))).Else(clientv3.OpGet(e.Key))
	if resp, err := e.txn.Commit(); err != nil {
		return err
	} else {
		if !resp.Succeeded {
			return MutexGrabFAIL
		}
		return nil
	}
FAIL:
	// 释放上下文，取消续租
	e.cancel()
	_, _ = e.lease.Revoke(ctx, e.leaseId) // 释放租约
	return err
}

/*
UnLock

	@Description: 释放锁
	@return error
*/
func (e *EtcdMutex) UnLock() error {
	if e.lock {
		e.cancel()
		if _, err := e.lease.Revoke(context.TODO(), e.leaseId); err != nil {
			zap.L().Info("释放锁异常")
			return fmt.Errorf("释放锁异常: %v", err)
		}
		return nil
	}
	return nil
}
