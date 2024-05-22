package etcd

import (
	"context"
	clientv3 "go.etcd.io/etcd/client/v3"
	recipe "go.etcd.io/etcd/client/v3/experimental/recipes"
)

/*
	etcd 普通队列
*/

type normalQ struct {
	client *clientv3.Client

	// 队列名称
	name string

	// 消费队列,普通队列
	Q *recipe.Queue

	// 锁
	mutex EMutex
}

type NormalQDO interface {
	Push(v []byte) error

	Pop() (string, error)

	// Len 获取队列长度
	Len() (int64, error)

	// WatcherWithPrefix watch 前缀为keyName的所有key
	WatcherWithPrefix(keyName string) clientv3.WatchChan
}

func NewNormalQ(name string) NormalQDO {
	if !etcdCLI.Invited() {
		return nil
	}
	if etcdCLI.Err() != nil {
		return nil
	}

	return &normalQ{
		client: etcdCLI.Client(),
		name:   name,
		Q:      recipe.NewQueue(etcdCLI.Client(), name),
		mutex:  NewMutex(name, 10),
	}
}

func (n *normalQ) Push(v []byte) error {
	return n.Q.Enqueue(string(v))
}

func (n *normalQ) Pop() (string, error) {
	return n.Q.Dequeue()
}

// Len 获取队列长度
func (n *normalQ) Len() (int64, error) {
	if resp, err := n.client.Get(context.TODO(), n.name, clientv3.WithPrefix(), clientv3.WithCountOnly()); err != nil {
		return 0, err
	} else {
		return resp.Count, nil
	}
}

// WatcherWithPrefix watch 前缀为keyName的所有key
func (n *normalQ) WatcherWithPrefix(keyName string) clientv3.WatchChan {
	return n.client.Watch(context.TODO(), keyName, clientv3.WithPrefix())
}
