package nats

import "time"

// URL/jsz?consumers=true

// JszConsumers 最外层JetStream统计信息结构体
type JszConsumers struct {
	Memory          uint64          `json:"memory"`           // 内存使用量（字节）
	Storage         uint64          `json:"storage"`          // 存储使用量（字节）
	ReservedMemory  uint64          `json:"reserved_memory"`  // 保留内存（字节）
	ReservedStorage uint64          `json:"reserved_storage"` // 保留存储（字节）
	Accounts        uint64          `json:"accounts"`         // 账户数量
	HaAssets        uint64          `json:"ha_assets"`        // HA资产数量
	Api             ApiStats        `json:"api"`              // API统计
	ServerId        string          `json:"server_id"`        // 服务器ID
	Now             time.Time       `json:"now"`              // 当前时间（ISO 8601）
	Config          JetStreamConfig `json:"config"`           // JetStream配置
	Limits          map[string]any  `json:"limits"`           // 动态限制（空对象用any）
	Streams         uint64          `json:"streams"`          // 流数量
	Consumers       uint64          `json:"consumers"`        // 消费者数量
	Messages        uint64          `json:"messages"`         // 消息总数
	Bytes           uint64          `json:"bytes"`            // 字节总数
	MetaCluster     MetaCluster     `json:"meta_cluster"`     // 元集群信息
	AccountDetails  []AccountDetail `json:"account_details"`  // 账户详情列表
	Total           uint64          `json:"total"`            // 总计（如账户总数）
}

// JetStreamConfig JetStream配置
type JetStreamConfig struct {
	MaxMemory    int64  `json:"max_memory"`  // 使用int64存储字节大小
	MaxStorage   int64  `json:"max_storage"` // 使用int64存储字节大小
	StoreDir     string `json:"store_dir"`
	SyncInterval int64  `json:"sync_interval"` // 使用int64存储纳秒时间
	CompressOk   bool   `json:"compress_ok"`
}

// ApiStats API统计结构体
type ApiStats struct {
	Level  uint64 `json:"level"`  // API级别
	Total  uint64 `json:"total"`  // 总请求量
	Errors uint64 `json:"errors"` // 错误请求数
}

// MetaCluster 元集群信息结构体
type MetaCluster struct {
	Name        string    `json:"name"`         // 集群名称
	Leader      string    `json:"leader"`       // 领导者节点
	Peer        string    `json:"peer"`         // 节点对等ID
	Replicas    []Replica `json:"replicas"`     // 副本列表
	ClusterSize uint64    `json:"cluster_size"` // 集群大小
	Pending     uint64    `json:"pending"`      // 待处理任务数
}

// Replica 副本信息结构体
type Replica struct {
	Name    string `json:"name"`    // 副本名称
	Current bool   `json:"current"` // 是否为当前副本
	Active  uint64 `json:"active"`  // 活跃状态（数值）
	Peer    string `json:"peer"`    // 节点对等ID
}

// AccountDetail 账户详情结构体
type AccountDetail struct {
	Name            string         `json:"name"`             // 账户名称
	Id              string         `json:"id"`               // 账户ID
	Memory          uint64         `json:"memory"`           // 账户内存使用量（字节）
	Storage         uint64         `json:"storage"`          // 账户存储使用量（字节）
	ReservedMemory  uint64         `json:"reserved_memory"`  // 账户保留内存（字节）
	ReservedStorage uint64         `json:"reserved_storage"` // 账户保留存储（字节）
	Accounts        uint64         `json:"accounts"`         // 子账户数量
	HaAssets        uint64         `json:"ha_assets"`        // HA资产数量
	Api             ApiStats       `json:"api"`              // 账户API统计
	StreamDetail    []StreamDetail `json:"stream_detail"`    // 流详情列表
}

// StreamDetail 流详情结构体
type StreamDetail struct {
	Name           string           `json:"name"`                      // 流名称
	Created        time.Time        `json:"created"`                   // 流创建时间（ISO 8601）
	Cluster        ClusterInfo      `json:"cluster"`                   // 流集群信息
	State          StreamState      `json:"state"`                     // 流状态
	ConsumerDetail []ConsumerDetail `json:"consumer_detail,omitempty"` // 消费者详情列表
}

// ClusterInfo 流集群信息结构体
type ClusterInfo struct {
	Name      string    `json:"name"`                 // 集群名称
	Leader    string    `json:"leader"`               // 领导者节点
	RaftGroup string    `json:"raft_group,omitempty"` // Raft组（可选）
	Replicas  []Replica `json:"replicas,omitempty"`   // 副本列表（可选）
}

// StreamState 流状态结构体
type StreamState struct {
	Messages      uint64    `json:"messages"`       // 消息总数
	Bytes         uint64    `json:"bytes"`          // 字节总数
	FirstSeq      uint64    `json:"first_seq"`      // 第一个序列号
	FirstTs       time.Time `json:"first_ts"`       // 第一条消息时间（ISO 8601）
	LastSeq       uint64    `json:"last_seq"`       // 最后一个序列号
	LastTs        time.Time `json:"last_ts"`        // 最后一条消息时间（ISO 8601）
	NumSubjects   uint64    `json:"num_subjects"`   // 主题数量
	ConsumerCount uint64    `json:"consumer_count"` // 消费者数量
}

// ConsumerDetail 消费者详情结构体
type ConsumerDetail struct {
	StreamName     string      `json:"stream_name"`     // 所属流名称
	Name           string      `json:"name"`            // 消费者名称
	Created        time.Time   `json:"created"`         // 消费者创建时间（ISO 8601）
	Delivered      Delivered   `json:"delivered"`       // 投递统计
	AckFloor       AckFloor    `json:"ack_floor"`       // 确认地板统计
	NumAckPending  uint64      `json:"num_ack_pending"` // 待确认消息数
	NumRedelivered uint64      `json:"num_redelivered"` // 重投递消息数
	NumWaiting     uint64      `json:"num_waiting"`     // 等待中的消费者数
	NumPending     uint64      `json:"num_pending"`     // 待处理消息数
	Cluster        ClusterInfo `json:"cluster"`         // 消费者集群信息
	PushBound      bool        `json:"push_bound"`      // 是否绑定推送
	Ts             time.Time   `json:"ts"`              // 最后更新时间（ISO 8601）
}

// Delivered 投递统计结构体
type Delivered struct {
	ConsumerSeq uint64    `json:"consumer_seq"`          // 消费者序列号
	StreamSeq   uint64    `json:"stream_seq"`            // 流序列号
	LastActive  time.Time `json:"last_active,omitempty"` // 最后活跃时间（可选）
}

// AckFloor 确认地板统计结构体
type AckFloor struct {
	ConsumerSeq uint64    `json:"consumer_seq"`          // 消费者确认序列号
	StreamSeq   uint64    `json:"stream_seq"`            // 流确认序列号
	LastActive  time.Time `json:"last_active,omitempty"` // 最后活跃时间（可选）
}
