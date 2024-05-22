package etcd

import (
	"context"
	clientv3 "go.etcd.io/etcd/client/v3"
)

/*
etcd 键值对的一些基本操作
*/

type EKV struct {
	client *clientv3.Client
	ctx    context.Context
}

type EKVDo interface {

	// Get 获取key的value
	Get(key string) (string, error)

	/*GetPrefix
	@Description: 查询所有key前缀符合的值
	@param prefix: 前缀
	*/
	GetPrefix(prefix string) ([]string, error)

	/*GetPrefixByte
	@Description: 查询所有key前缀符合的值
	@param prefix: 前缀
	@return []string
	@return error
	*/
	GetPrefixByte(prefix string) ([][]byte, error)

	GetPrefixWithSerializable(prefix string) (*clientv3.GetResponse, error)

	/*PutWithLease
	@Description: 写入带租约的键值，不带自动续租，到期失效
	@param ttl 租约时间
	@return error
	*/
	PutWithLease(key string, value string, ttl int64) error

	/*Put
	@Description: 写入不带租约的键值
	*/
	Put(key string, value string) error

	// Del 普通删除
	Del(key string) error

	// DelPrefix 基于前缀删除
	DelPrefix(prefix string) error

	/*PrefixLen
	@Description: 符合前缀的键值数量
	@return int64
	*/
	PrefixLen(prefix string) (int64, error)
}

func NewEKVDo() EKVDo {
	if !etcdCLI.invited {
		return nil
	}
	if etcdCLI.Err() != nil {
		return nil
	}
	return &EKV{
		client: etcdCLI.Client(),
		ctx:    context.Background(),
	}
}

// Get 普通查询,需要指定key
func (e *EKV) Get(key string) (string, error) {
	if resp, err := e.client.Get(e.ctx, key); err != nil {
		return "", err
	} else {
		return string(resp.Kvs[0].Value), nil
	}
}

/*
GetPrefix

	@Description: 查询所有key前缀符合的值
	@param prefix: 前缀
	@return []string
	@return error
*/
func (e *EKV) GetPrefix(prefix string) ([]string, error) {
	var res = make([]string, 0)
	resp, err := e.client.Get(e.ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	for _, v := range resp.Kvs {
		res = append(res, string(v.Value))
	}
	return res, nil
}

/*
GetPrefixByte

	@Description: 查询所有key前缀符合的值
	@param prefix: 前缀
	@return []string
	@return error
*/
func (e *EKV) GetPrefixByte(prefix string) ([][]byte, error) {
	var res = make([][]byte, 0)
	resp, err := e.client.Get(e.ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	for _, v := range resp.Kvs {
		res = append(res, v.Value)
	}
	return res, nil
}

func (e *EKV) GetPrefixWithSerializable(prefix string) (*clientv3.GetResponse, error) {
	return e.client.Get(e.ctx, prefix, clientv3.WithPrefix(), clientv3.WithSerializable())
}

/*
PutWithLease

	@Description: 写入带租约的键值，不带自动续租，到期失效
	@param ttl 租约时间
	@return error
*/
func (e *EKV) PutWithLease(key string, value string, ttl int64) error {
	leaseGrant, _ := e.client.Grant(e.ctx, ttl)
	if _, err := e.client.Put(e.ctx, key, value, clientv3.WithLease(leaseGrant.ID)); err != nil {
		return err
	}
	return nil
}

/*
Put

	@Description: 写入不带租约的键值
*/
func (e *EKV) Put(key string, value string) error {
	if _, err := e.client.Put(e.ctx, key, value); err != nil {
		return err
	}
	return nil
}

// Del 普通删除,需要指定key
func (e *EKV) Del(key string) error {
	_, err := e.client.Delete(e.ctx, key)
	return err
}

// DelPrefix 基于前缀删除
func (e *EKV) DelPrefix(prefix string) error {
	_, err := e.client.Delete(e.ctx, prefix, clientv3.WithPrefix())
	return err
}

/*
PrefixLen
@Description: 符合前缀的键值数量
@return int64
*/
func (e *EKV) PrefixLen(prefix string) (int64, error) {
	res, err := e.client.Get(e.ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	return res.Count, err
}
