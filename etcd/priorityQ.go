package etcd

import (
	"context"
	"encoding/json"
	clientv3 "go.etcd.io/etcd/client/v3"
	recipe "go.etcd.io/etcd/client/v3/experimental/recipes"
)

/*
	etcd 优先队列
*/

type (
	priorityQ struct {
		client *clientv3.Client

		// 等待队列key的名称
		name string

		// 等待队列,优先队列
		Q *recipe.PriorityQueue

		// 置顶任务写入等待队列使用的优先指数
		topPr uint16

		// 普通任务写入等待队列使用的优先指数
		waitingPr uint16
	}
)

type PriorityQDO interface {

	// GetTopPr
	//
	// @Description: 置顶任务的优先指数
	GetTopPr() uint16

	// GetWaitPr
	//
	// @Description: 普通任务的优先指数
	GetWaitPr() uint16

	// Push
	//
	// @Description: 以指定的优先指数向优先队列添加
	//
	// @param v 值
	//
	// @param pr 优先指数
	//
	// @return error
	Push(v string, pr uint16) error

	// Pop 从等待队列中取出,以字符串形式输出结果
	Pop() (string, error)

	// Len 获取队列长度
	Len() (int64, error)

	// GetLastQ
	//
	// @Description: 获取队列最后一个
	//
	// @return []byte 队列中的key值
	//
	// @return []byte value
	GetLastQ() ([]byte, []byte)

	// WatcherWithPrefix watch 前缀为keyName的所有key
	WatcherWithPrefix(keyName string) clientv3.WatchChan

	/*PushStruct
	  @Description: 向优先队列添加 struct
	  @param target: 需要写入的结构体对象
	  @param pr: 优先指数
	  @return error
	*/
	PushStruct(task interface{}, pr uint16) error

	/*PopStruct
	@Description: 从优先队列中弹出,以 struct 形式输出结果
	@param target: 弹出结果的接收对象
	@return interface{}: 弹出的结果
	@return error
	*/
	PopStruct(target interface{}) (interface{}, error)
}

/*
NewWPriorityQ

	@Description: 优先队列
	@param name: 队列名称
	@param topPr: 任务置顶时使用的优先级指数
	@param waitingPr: 普通任务使用的优先级指数
	@return PriorityQDO
*/
func NewWPriorityQ(name string, topPr uint16, waitingPr uint16) PriorityQDO {
	if !etcdCLI.Invited() {
		return nil
	}
	if etcdCLI.Err() != nil {
		return nil
	}

	return &priorityQ{
		client:    etcdCLI.Client(),
		name:      name,
		Q:         recipe.NewPriorityQueue(etcdCLI.Client(), name),
		topPr:     topPr,
		waitingPr: waitingPr,
	}
}

/*
GetTopPr

	@Description: 置顶任务的优先指数
*/
func (p *priorityQ) GetTopPr() uint16 {
	return p.topPr
}

/*
GetWaitPr

	@Description: 普通任务的优先指数
*/
func (p *priorityQ) GetWaitPr() uint16 {
	return p.waitingPr
}

/*
Push

	@Description: 以指定的优先指数向优先队列添加
	@param v 值
	@param pr 优先指数
	@return error
*/
func (p *priorityQ) Push(v string, pr uint16) error {
	return p.Q.Enqueue(v, pr)
}

// Pop 从等待队列中取出,以字符串形式输出结果
func (p *priorityQ) Pop() (string, error) {
	return p.Q.Dequeue()
}

// Len 获取队列长度
func (p *priorityQ) Len() (int64, error) {
	if resp, err := p.client.Get(context.TODO(), p.name, clientv3.WithPrefix(), clientv3.WithCountOnly()); err != nil {
		return 0, err
	} else {
		return resp.Count, nil
	}
}

/*
GetLastQ

	@Description: 获取队列最后一个
	@return []byte 队列中的key值
	@return []byte value
*/
func (p *priorityQ) GetLastQ() ([]byte, []byte) {
	if resp, err := p.client.Get(context.TODO(), p.name, clientv3.WithLastKey()...); err != nil {
		return nil, nil
	} else {
		for _, v := range resp.Kvs {
			return v.Key, v.Value
		}
	}
	return nil, nil
}

// WatcherWithPrefix watch 前缀为keyName的所有key
func (p *priorityQ) WatcherWithPrefix(keyName string) clientv3.WatchChan {
	return p.client.Watch(context.TODO(), keyName, clientv3.WithPrefix())
}

/*
PushStruct

	@Description: 向优先队列添加 struct
	@param target: 需要写入的结构体对象
	@param pr: 优先指数
	@return error
*/
func (p *priorityQ) PushStruct(target interface{}, pr uint16) error {
	b, err := json.Marshal(&target)
	if err != nil {
		return err
	}
	return p.Q.Enqueue(string(b), pr)
}

/*
PopStruct

	@Description: 从优先队列中弹出,以 struct 形式输出结果
	@param target: 弹出结果的接收对象
	@return interface{}: 弹出的结果
	@return error
*/
func (p *priorityQ) PopStruct(target interface{}) (interface{}, error) {
	if s, err := p.Pop(); err != nil {
		return nil, err
	} else {
		if err = json.Unmarshal([]byte(s), &target); err != nil {
			return nil, err
		}
		return target, nil
	}
}
