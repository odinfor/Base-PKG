package nacos

import (
	"fmt"
	"github.com/nacos-group/nacos-sdk-go/clients"
	"github.com/nacos-group/nacos-sdk-go/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/common/constant"
	"github.com/nacos-group/nacos-sdk-go/model"
	"github.com/nacos-group/nacos-sdk-go/vo"
)

type (
	nacosClient struct {
		clientConfig  constant.ClientConfig
		serverConfigs []constant.ServerConfig
		namingClient  *naming_client.INamingClient
		configClient  *config_client.IConfigClient
	}

	NacosDO interface {

		/*SetLogDir
		@Description: 修改nacos 日志存储地址
		@param path 存储日志路径
		*/
		SetLogDir(path string)

		/*SetCacheDir
		@Description: 修改nacos缓存存储地址
		@param path 存储缓存路径
		*/
		SetCacheDir(path string)

		/*SetTimeout
		@Description: 修改nacos连接超时时间
		@param num 设置超时时间,单位毫秒
		*/
		SetTimeout(num uint64)

		// CreateNamingClient
		// @Description: 创建连接客户端
		CreateNamingClient() error

		/*CreateConfigClient
		@Description: 创建配置客户端
		*/
		CreateConfigClient() error

		GetNamingClient() *naming_client.INamingClient

		GetConfigClient() *config_client.IConfigClient

		/*RegisterInstance
		@Description: 服务注册到nacos
		*/
		RegisterInstance(nacosIp string, serviceName string, groupName string, clusterName string) error

		/*DeregisterInstance
		@Description: 取消注册
		*/
		DeregisterInstance(nacosIp string, serviceName string, groupName string, clusterName string) error

		/*GetService
		@Description: 获取服务信息
		@param serviceName 服务名称
		@param cluster 集群名称
		@param groupName 组名
		@return model.Service 服务信息
		@return error
		*/
		GetService(serviceName string, cluster []string, groupName string) (model.Service, error)

		/*GetConfig
		@Description: 获取配置,建议配置用json,可直接绑定到结构体
		@param dataId 配置的data id
		@param group
		@return string
		@return error
		*/
		GetConfig(dataId string, group string) (string, error)

		/*ListenConfig
		@Description: 监听配置修改事件
		@param dataId
		@param group
		@return error
		*/
		ListenConfig(dataId string, group string)

		/*CancelListenConfig
		@Description: 取消监听事件
		*/
		CancelListenConfig(dataId string, group string) error

		/*PublishConfig
		@Description: 发布配置
		*/
		PublishConfig(dataId string, group string, content string) error

		/*DeleteConfig
		@Description: 删除配置
		*/
		DeleteConfig(dataId string, group string) error
	}
)

/*
NewNacosClient

	@Description: nacos客户端,部分参数默认配置,需要更改自行调用对应方法
	@param namespaceId: nacos 命名空间
	@param username nacos用户名
	@param password nacos密码
	@param ipAddr nacos 集群ip
	@return *nacosClient
*/
func NewNacosClient(namespaceId string, username string, password string, ipAddr []string) NacosDO {
	var serverConfigs []constant.ServerConfig
	for _, v := range ipAddr {
		var c constant.ServerConfig
		c.IpAddr = v
		c.Port = 8848
		c.Scheme = "http"

		serverConfigs = append(serverConfigs, c)
	}
	return &nacosClient{
		clientConfig: constant.ClientConfig{
			NamespaceId:         namespaceId,
			TimeoutMs:           5000,
			NotLoadCacheAtStart: true,
			Username:            username,
			Password:            password,
		},
		serverConfigs: serverConfigs,
	}
}

// SetLogDir
// @Description: 修改nacos 日志存储地址
// @param path
func (n *nacosClient) SetLogDir(path string) {
	n.clientConfig.LogDir = path
}

// SetCacheDir
// @Description: 修改nacos缓存存储地址
// @param path
func (n *nacosClient) SetCacheDir(path string) {
	n.clientConfig.CacheDir = path
}

// SetTimeout
// @Description: 修改nacos连接超时时间
// @param num 设置超时时间,单位毫秒
func (n *nacosClient) SetTimeout(num uint64) {
	n.clientConfig.TimeoutMs = num
}

// CreateNamingClient
// @Description: 创建连接客户端
func (n *nacosClient) CreateNamingClient() error {
	namingClient, err := clients.NewNamingClient(vo.NacosClientParam{
		ClientConfig:  &n.clientConfig,
		ServerConfigs: n.serverConfigs,
	})
	if err != nil {
		return err
	}
	n.namingClient = &namingClient
	return nil
}

// CreateConfigClient
// @Description: 创建配置客户端
// @return error
func (n *nacosClient) CreateConfigClient() error { //
	configClient, err := clients.NewConfigClient(vo.NacosClientParam{
		ClientConfig:  &n.clientConfig,
		ServerConfigs: n.serverConfigs,
	})
	if err != nil {
		return err
	}
	n.configClient = &configClient
	return nil
}

func (n *nacosClient) GetNamingClient() *naming_client.INamingClient {
	return n.namingClient
}

func (n *nacosClient) GetConfigClient() *config_client.IConfigClient {
	return n.configClient
}

// RegisterInstance
// @Description: 服务注册到nacos
// @receiver n
// @param nacosIp
// @param serviceName
// @param groupName
// @param clusterName
// @return error
func (n *nacosClient) RegisterInstance(nacosIp string, serviceName string, groupName string, clusterName string) error {
	client := *n.namingClient
	success, err := client.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          nacosIp,
		Port:        8848,
		ServiceName: serviceName,
		GroupName:   groupName,
		Weight:      10,
		ClusterName: clusterName,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
	})
	if !success {
		return err
	}
	return nil
}

// DeregisterInstance
// @Description: 取消注册
// @param nacosIp
// @param serviceName
// @param groupName
// @param clusterName
// @return error
func (n *nacosClient) DeregisterInstance(nacosIp string, serviceName string, groupName string, clusterName string) error {
	client := *n.namingClient

	success, err := client.DeregisterInstance(vo.DeregisterInstanceParam{
		Ip:          nacosIp,
		Port:        8848,
		ServiceName: serviceName,
		Ephemeral:   true,
		Cluster:     clusterName, // default value is DEFAULT
		GroupName:   groupName,   // default value is DEFAULT_GROUP
	})
	if !success {
		return err
	}
	return nil
}

// GetService
// @Description: 获取服务信息
// @param serviceName 服务名称
// @param cluster 集群名称
// @param groupName 组名
// @return model.Service 服务信息
// @return error
func (n *nacosClient) GetService(serviceName string, cluster []string, groupName string) (model.Service, error) {
	client := *n.namingClient
	services, err := client.GetService(vo.GetServiceParam{
		ServiceName: serviceName,
		Clusters:    cluster,
		GroupName:   groupName,
	})
	if err != nil {
		return model.Service{}, err
	}
	return services, nil
}

// GetConfig
// @Description: 获取配置,建议配置用json,可直接绑定到结构体
// @param dataId 配置的data id
// @param group
// @return string
// @return error
func (n *nacosClient) GetConfig(dataId string, group string) (string, error) {
	client := *n.configClient
	content, err := client.GetConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  group,
	})
	if err != nil {
		return "", err
	}
	return content, nil
}

// ListenConfig
// @Description: 监听配置修改事件
// @param dataId
// @param group
// @return error
func (n *nacosClient) ListenConfig(dataId string, group string) {
	_ = n.CreateConfigClient()
	client := *n.configClient
	err := client.ListenConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  group,
		OnChange: func(namespace, group, dataId, data string) {
			fmt.Println("group:" + group + ", dataId:" + dataId + ", data:" + data)
		},
	})
	if err != nil {
		return
	}
	return
}

// CancelListenConfig
// @Description: 取消监听事件
// @param dataId
// @param group
// @return error
func (n *nacosClient) CancelListenConfig(dataId string, group string) error {
	client := *n.configClient
	err := client.CancelListenConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  group,
	})
	if err != nil {
		return err
	}
	return nil
}

// PublishConfig
// @Description: 发布配置
// @param dataId
// @param group
// @return error
func (n *nacosClient) PublishConfig(dataId string, group string, content string) error {
	client := *n.configClient
	success, err := client.PublishConfig(vo.ConfigParam{
		DataId:  dataId,
		Group:   group,
		Content: content,
	})
	if !success {
		return err
	}
	return nil
}

// DeleteConfig
// @Description: 删除配置
// @param dataId
// @param group
// @return error
func (n *nacosClient) DeleteConfig(dataId string, group string) error {
	client := *n.configClient
	success, err := client.DeleteConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  group,
	})
	if !success {
		return err
	}
	return nil
}
