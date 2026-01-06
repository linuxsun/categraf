package nats

import (
	"encoding/json"
	"flashcat.cloud/categraf/config"
	"flashcat.cloud/categraf/inputs"
	"flashcat.cloud/categraf/types"
	gnatsd "github.com/nats-io/nats-server/v2/server"
	"io"
	"log"
	"net/http"
	"net/url"
	"path"
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
}

func (ins *Instance) Init() error {
	if ins.Server == "" {
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
	err := ins.healthMetrics(slist)
	if err != nil {
		return
	}
	err = ins.customMetrics(slist)
	if err != nil {
		return
	}
	err = ins.defaultMetrics(slist)
	if err != nil {
		return
	}
}

// healthMetrics 健康状态监控指标
func (ins *Instance) healthMetrics(lists *types.SampleList) error {
	tags := map[string]string{
		"server": ins.Server,
	}
	address, err := url.Parse(ins.Server)
	if err != nil {
		log.Println("E! error parseURL", err)
		lists.PushSample(inputName, "healthz", 0, tags)
		return err
	}
	address.Path = path.Join(address.Path, "varz")
	resp, err := ins.client.Get(address.String())
	addr, err := url.Parse(ins.Server)
	if err != nil {
		log.Println("E! error parseURL", err)
		lists.PushSample(inputName, "healthz", 0, tags)
		return err
	}
	u, err := url.Parse(ins.Server)
	if err != nil {
		log.Println("E! error parseURL", err)
		lists.PushSample(inputName, "healthz", 0, tags)
		return err
	}
	u.Path = path.Join(u.Path, "/jsz")
	u.RawQuery = "consumers=true"
	resp, err = ins.client.Get(u.String())
	//log.Println(u.String(), resp)
	if err != nil {
		log.Println("E! error while polling", addr.String(), err)
		lists.PushSample(inputName, "healthz", 0, tags)
		return err
	}

	defer resp.Body.Close()
	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println("E! error reading body", err)
		lists.PushSample(inputName, "healthz", 0, tags)
		return err
	}

	jsz := JszConsumers{}
	err = json.Unmarshal(bytes, &jsz)
	if err != nil {
		log.Println("E! error parsing response", err)
		lists.PushSample(inputName, "healthz", 0, tags)
		return err
	}

	lists.PushSample(inputName, "healthz", 1, tags) //健康检查

	return nil
}

// customMetrics 自定义监控指标；
// nats_streams_total 流数量；
// nats_consumers_total 消费者数量；
// nats_msg_total 消息数量/积压；
// nats_stream_msg_count Streams 对应消息数量；
// nats_stream_consumer_count Streams 对应消费者数量。
func (ins *Instance) customMetrics(lists *types.SampleList) error {
	u, err := url.Parse(ins.Server)
	if err != nil {
		log.Println("E! error parseURL", err)
		return err
	}
	u.Path = path.Join(u.Path, "/jsz")
	u.RawQuery = "consumers=true"
	resp, err := ins.client.Get(u.String())
	if err != nil {
		log.Println("E! error while polling", u.String(), err)
		return err
	}

	defer resp.Body.Close()
	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println("E! error reading body", err)
		return err
	}

	jsz := JszConsumers{}
	err = json.Unmarshal(bytes, &jsz)
	if err != nil {
		log.Println("E! error parsing response", err)
		return err
	}

	tags := map[string]string{
		"server": ins.Server,
	}
	lists.PushSample(inputName, "streams_total", jsz.Streams, tags)     // 流数量
	lists.PushSample(inputName, "consumers_total", jsz.Consumers, tags) // 消费者数量
	lists.PushSample(inputName, "msg_total", jsz.Messages, tags)        // 消息数量/积压

	if len(jsz.AccountDetails) == 0 {
		return nil
	}

	for _, stream := range jsz.AccountDetails[0].StreamDetail {
		myTags := map[string]string{"server": ins.Server, "stream_name": stream.Name}
		lists.PushSample(inputName, "stream_msg_count", stream.State.Messages, myTags)
		lists.PushSample(inputName, "stream_consumer_count", stream.State.ConsumerCount, myTags)
	}

	return nil
}

// defaultMetrics 默认监控指标
func (ins *Instance) defaultMetrics(slist *types.SampleList) error {
	if ins.DebugMod {
		log.Println("D! nats... server:", ins.Server)
	}
	address, err := url.Parse(ins.Server)
	if err != nil {
		log.Println("E! error parseURL", err)
		return err
	}
	address.Path = path.Join(address.Path, "varz")

	resp, err := ins.client.Get(address.String())
	if err != nil {
		log.Println("E! error while polling", address.String(), err)
		return err
	}
	defer resp.Body.Close()

	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println("E! error reading body", err)
		return err
	}

	stats := new(gnatsd.Varz)
	err = json.Unmarshal(bytes, &stats)
	if err != nil {
		log.Println("E! error parsing response", err)
		return err
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

	return nil
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
