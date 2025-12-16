package nats

import (
	"encoding/json"
	"flashcat.cloud/categraf/config"
	"flashcat.cloud/categraf/inputs"
	"flashcat.cloud/categraf/types"
	gnatsd "github.com/nats-io/nats-server/v2/server"
	"go.uber.org/atomic"
	"io"
	"log"
	"net/http"
	"net/url"
	"path"
	"sync"
	"time"
)

const inputName = "nats"

type Nats struct {
	config.PluginConfig
	Instances []*Instance `toml:"instances"`
}

func init() {
	inputs.Add(inputName, func() inputs.Input {
		return &Nats{}
	})
}

func (n *Nats) Clone() inputs.Input {
	return &Nats{}
}

func (n *Nats) Name() string {
	return inputName
}

func (n *Nats) GetInstances() []inputs.Instance {
	ret := make([]inputs.Instance, len(n.Instances))
	for i := 0; i < len(n.Instances); i++ {
		ret[i] = n.Instances[i]
	}
	return ret
}

type Instance struct {
	Server          string          `toml:"server"`
	ResponseTimeout config.Duration `toml:"response_timeout"`

	client *http.Client
	config.HTTPCommonConfig
	config.InstanceConfig

	//监控nats集群，添加自定义监控指标
	mutex        sync.RWMutex
	NatServer    *nats.Conn
	NatsUser     string   `toml:"user"`
	NatsPassword string   `toml:"password"`
	NatsCluster  []string `toml:"cluster"`
	//NatsHealth     atomic.Bool  `toml:"health"`
	StreamsTotal   atomic.Int64 `toml:"streams_total"`
	ConsumersTotal atomic.Int64 `toml:"consumers_total"`
	MsgTotal       atomic.Int64 `toml:"msg_total"`
}

func (ins *Instance) Init() error {
	if ins.Server == "" || ins.NatsCluster == nil {
		return types.ErrInstancesEmpty
	}
	if ins.ResponseTimeout <= 0 {
		ins.ResponseTimeout = config.Duration(time.Second * 5)
	}

	ins.InitHTTPClientConfig()

	var err error
	ins.client, err = ins.createHTTPClient()
	return err
}

func (ins *Instance) Gather(slist *types.SampleList) {
	//监控nats集群，添加自定义监控指标
	stream := NewJetStream(ins)
	defer stream.CloseJetStreams() // 确保函数退出时总是关闭连接
	//file, _ := stream.getConfigPath()
	//_ = stream.initConfig(ins, file)
	stream.ConnectNats(ins)
	stream.SetNatsHealthz(slist, ins)
	stream.GetListStreams(ins, slist)

	if ins.DebugMod {
		log.Println("D! nats... server:", ins.Server)
	}
	address, err := url.Parse(ins.Server)
	if err != nil {
		log.Println("E! error parseURL", err)
		return
	}
	address.Path = path.Join(address.Path, "varz")

	resp, err := ins.client.Get(address.String())
	if err != nil {
		log.Println("E! error while polling", address.String(), err)
		return
	}
	defer resp.Body.Close()

	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println("E! error reading body", err)
		return
	}

	stats := new(gnatsd.Varz)
	err = json.Unmarshal(bytes, &stats)
	if err != nil {
		log.Println("E! error parsing response", err)
		return
	}

	fields := map[string]interface{}{
		"in_msgs":           stats.InMsgs,
		"out_msgs":          stats.OutMsgs,
		"in_bytes":          stats.InBytes,
		"out_bytes":         stats.OutBytes,
		"uptime":            stats.Now.Sub(stats.Start).Nanoseconds(),
		"cores":             stats.Cores,
		"cpu":               stats.CPU,
		"mem":               stats.Mem,
		"connections":       stats.Connections,
		"total_connections": stats.TotalConnections,
		"subscriptions":     stats.Subscriptions,
		"slow_consumers":    stats.SlowConsumers,
		"routes":            stats.Routes,
		"remotes":           stats.Remotes,
	}
	tags := map[string]string{
		"server": ins.Server,
	}
	slist.PushSamples(inputName, fields, tags)

}

func (ins *Instance) createHTTPClient() (*http.Client, error) {
	tr := &http.Transport{
		ResponseHeaderTimeout: time.Duration(ins.ResponseTimeout),
	}

	client := &http.Client{
		Transport: tr,
		Timeout:   time.Duration(ins.ResponseTimeout),
	}
	return client, nil
}
