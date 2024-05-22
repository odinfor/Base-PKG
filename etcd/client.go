package etcd

import (
	"context"
	"errors"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"time"
)

var etcdCLI = &cli{ctx: context.Background()}

type cli struct {
	client  *clientv3.Client
	err     error
	ctx     context.Context
	invited bool
}

type EtcdDO interface {
	Err() error

	Invited() bool

	Client() *clientv3.Client
}

func NewCli() EtcdDO {
	return etcdCLI
}

/*
InitClient

	@Description: 初始化etcd客户端，建立连接
	@param endPoints: etcd集群节点
*/
func InitClient(endPoints []string) {
	var (
		err        error
		statusResp *clientv3.StatusResponse
	)

	if len(endPoints) == 0 {
		etcdCLI.err = errors.New("endpoints 不能为空")
		return
	}

	etcdCLI.client, etcdCLI.err = clientv3.New(clientv3.Config{
		Endpoints:   endPoints,
		DialTimeout: time.Duration(3) * time.Second,
		LogConfig: &zap.Config{
			Level:       zap.NewAtomicLevelAt(zapcore.ErrorLevel),
			Development: false,
			Sampling: &zap.SamplingConfig{
				Initial:    100,
				Thereafter: 100,
			},
			Encoding:         "json",
			EncoderConfig:    zap.NewProductionEncoderConfig(),
			OutputPaths:      []string{"stderr"},
			ErrorOutputPaths: []string{"stderr"},
		},
	})

	if etcdCLI.err != nil {
		return
	}

	// v3版本使用的新的均衡器带来的问题,连接失败不会抛出任何异常，需要主动通过status状态检查
	timeoutCtx, cancel := context.WithTimeout(etcdCLI.ctx, 2*time.Second)
	defer cancel()
	for retryTime := 0; retryTime < 3; retryTime++ {
		if statusResp, etcdCLI.err = etcdCLI.client.Status(timeoutCtx, endPoints[0]); err != nil {
			fmt.Println(err)
			fmt.Println("连接etcd失败......，正在尝试重连......")
			time.Sleep(time.Second * 1)
		} else {
			fmt.Print("etcd集群信息: ")
			fmt.Println(statusResp)
			break
		}
	}
	if etcdCLI.err != nil {
		etcdCLI.err = errors.New("etcd 连接失败")
	}

	etcdCLI.invited = true
}

func (c *cli) Err() error {
	return c.err
}

func (c *cli) Invited() bool {
	return c.invited
}

func (c *cli) Client() *clientv3.Client {
	return c.client
}
