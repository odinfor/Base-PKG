package etcd

import (
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	recipe "go.etcd.io/etcd/client/v3/experimental/recipes"
	"testing"
)

type Task struct {
	// 调度表id
	ScheduleId uint `json:"scheduleId"`

	// 批次码，用于区分是否是同一批次发布的任务
	BatchCode int64 `json:"batchCode"`

	// 批次号,同一批次中发布的任务,链式发布基于批次号区分先后顺序
	BatchId int `json:"batchId"`

	// 加入发布时间
	Time string `json:"time"`

	// 执行时间
	RunTime string `json:"runTime"`

	// 发布模式
	PublishModel int `json:"publishModel"`

	// 工单id
	WorkOrderId uint `json:"workOrderId"`
}

func Test_priorityQ_PushStruct(t *testing.T) {
	type fields struct {
		client    *clientv3.Client
		name      string
		Q         *recipe.PriorityQueue
		topPr     uint16
		waitingPr uint16
	}
	type args struct {
		task interface{}
		pr   uint16
	}
	InitClient([]string{"172.20.5.63:12380"})
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "测试 push struct",
			fields: fields{
				client:    NewCli().Client(),
				name:      "test_pri",
				Q:         recipe.NewPriorityQueue(etcdCLI.Client(), "test_pri"),
				topPr:     10,
				waitingPr: 50,
			},
			args: args{
				task: Task{
					ScheduleId:   10,
					BatchCode:    21748678642,
					BatchId:      1,
					Time:         "2023-1-2 12:23:44",
					RunTime:      "",
					PublishModel: 4,
					WorkOrderId:  11023,
				},
				pr: 50,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &priorityQ{
				client:    tt.fields.client,
				name:      tt.fields.name,
				Q:         tt.fields.Q,
				topPr:     tt.fields.topPr,
				waitingPr: tt.fields.waitingPr,
			}
			if err := p.PushStruct(tt.args.task, tt.args.pr); (err != nil) != tt.wantErr {
				t.Errorf("PushStruct() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_priorityQ_PopStruct1(t *testing.T) {
	type fields struct {
		client    *clientv3.Client
		name      string
		Q         *recipe.PriorityQueue
		topPr     uint16
		waitingPr uint16
	}
	type args struct {
		target interface{}
	}
	InitClient([]string{"172.20.5.63:12380"})
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "test get of struct",
			fields: fields{
				client:    NewCli().Client(),
				name:      "test_pri",
				Q:         recipe.NewPriorityQueue(etcdCLI.Client(), "test_pri"),
				topPr:     10,
				waitingPr: 50,
			},
			args:    args{target: Task{}},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &priorityQ{
				client:    tt.fields.client,
				name:      tt.fields.name,
				Q:         tt.fields.Q,
				topPr:     tt.fields.topPr,
				waitingPr: tt.fields.waitingPr,
			}
			got, err := p.PopStruct(tt.args.target)
			if (err != nil) != tt.wantErr {
				t.Errorf("PopStruct() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			fmt.Println(got)
		})
	}
}
