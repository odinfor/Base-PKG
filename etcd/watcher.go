package etcd

import (
	"context"
	"errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"time"
)

type (
	watcher struct {
		ctx context.Context

		//kv EKVDo

		cli EtcdDO

		// mtx 竞争锁
		mtx EMutex
	}

	putFunc func(event *clientv3.Event)

	delFunc func()

	WatcherDO interface {

		/*PutF
		@Description: 监听到 key put 事件后需要执行的方法
		@param event: 监听到的事件
		@return putFunc
		*/
		PutF() putFunc

		// DelF 监听到 key del 事件后需要执行的方法
		DelF() delFunc

		/*Watcher
		@Description: etcd 监听器
		@param prefix: 前缀key
		@param putFunc: put 事件执行器
		@param delFunc: del 事件执行器
		*/
		Watcher(prefix string, putFunc putFunc, delFunc delFunc)
	}
)

/*
NewWatcher

	@Description: etcd 监听器
	@param mtx: 竞争锁
	@return WatcherDO
*/
func NewWatcher(mtx EMutex) WatcherDO {
	return watcher{
		ctx: context.Background(),
		mtx: mtx,
		cli: NewCli(),
	}
}

/*
PutF

	@Description: 监听到 key put 事件后需要执行的方法
	@param event: 监听到的事件
	@return putFunc
*/
func (w watcher) PutF() putFunc {
	return func(event *clientv3.Event) {}
}

// DelF 监听到 key del 事件后需要执行的方法
func (w watcher) DelF() delFunc {
	return func() {}
}

/*
Watcher

	@Description: etcd 监听器
	@param prefix: 前缀key
	@param putFunc: put 事件执行器
	@param delFunc: del 事件执行器
*/
func (w watcher) Watcher(prefix string, putFunc putFunc, delFunc delFunc) {
	wtr := w.cli.Client().Watch(context.TODO(), prefix, clientv3.WithPrefix()) // watch 以ExecQueueKey开头的
	for res := range wtr {
		for _, event := range res.Events {
			switch event.Type {
			case clientv3.EventTypePut: // 执行列表写入事件,调用jenkins build task
				func() {
					zap.L().Info("etcd watch put key: " + string(event.Kv.Key) + ": " + string(event.Kv.Value))
					// 抢锁执行
					err := w.mtx.Lock(time.Now().String())
					defer func(sMutex EMutex) {
						if err = sMutex.UnLock(); err != nil {
							zap.L().Error("etcd mutex unlock found error", zap.Error(err))
						}
					}(w.mtx)

					switch {
					case err == nil:
						// 抢到锁后的执行流程 - 监听到添加事件的执行方法
						putFunc(event)
					case errors.Is(err, MutexGrabFAIL):
						zap.L().Info("抢锁失败", zap.String("kv", event.Kv.String()))
						return
					default:
						zap.L().Error("抢锁发现异常", zap.Error(err))
						return
					}
				}()
			case clientv3.EventTypeDelete: // 执行列表删除事件.从等待队列中pop出一个task加入到执行列表中,同时删除存储在等待队列中完整key的kv
				func() {
					zap.L().Info("etcd watch delete key: " + string(event.Kv.Key))
					// 抢锁执行
					err := w.mtx.Lock(time.Now().String())
					if err != nil && err.Error() != "抢锁失败" {
						zap.L().Error("抢锁发现异常", zap.Error(err))
						return
					}
					if err != nil && err.Error() == "抢锁失败" {
						zap.L().Info("抢锁失败, exec key:" + string(event.Kv.Key))
						return
					}
					if err == nil {
						zap.L().Info("抢锁成功, exec key:" + string(event.Kv.Key))
					}
					defer func(sMutex EMutex) {
						if err = sMutex.UnLock(); err != nil {
							zap.L().Error("etcd mutex unlock found error", zap.Error(err))
						}
					}(w.mtx)

					// 抢到锁后的执行流程 - 监听到删除事件的执行方法
					delFunc()
				}()
			}
		}
	}
}
