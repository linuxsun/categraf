package nats

import (
	"context"
	"encoding/json"
	"errors"
	"flashcat.cloud/categraf/types"
	"fmt"
	"github.com/BurntSushi/toml"
	gnatsd "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
	"io"
	"log"
	"net/url"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"
)

type JetStreamInter interface {
	ConnectNats(ins *Instance) (*Instance, error)
	GetListStreams(ins *Instance, lst *types.SampleList) error
	SetNatsHealthz(lst *types.SampleList, ins *Instance) error
	getConfigPath() (string, error)
	initConfig(ins *Instance, f string) error
	CloseJetStreams()
}

func NewJetStream(ins *Instance) JetStreamInter {
	return &Instance{
		NatServer:      nil,
		NatsUser:       ins.NatsUser,
		NatsPassword:   ins.NatsPassword,
		NatsCluster:    ins.NatsCluster,
		StreamsTotal:   ins.StreamsTotal,
		ConsumersTotal: ins.ConsumersTotal,
		MsgTotal:       ins.MsgTotal,
	}
}

type TaskPool struct {
	taskChan  chan Task
	close     chan struct{}
	closeOnce sync.Once
}

type Task func()

func NewTaskPool(numG int, capacity int) *TaskPool {
	res := &TaskPool{
		taskChan: make(chan Task, capacity),
		close:    make(chan struct{}),
		//close:    atomic.NewBool(false),
	}

	if numG <= 0 || numG > 10000 {
		numG = 10
	}
	if capacity <= 0 || capacity > 1000000 {
		capacity = 100
	}

	for i := 0; i < numG; i++ {
		go func() {
			for {
				select {
				case <-res.close:
					return
				case t := <-res.taskChan:
					t()
				}
			}
		}()
	}

	return res
}

// Submit 提交任务
func (p *TaskPool) Submit(ctx context.Context, t Task) error {
	//p.taskChan <- t
	select {
	case p.taskChan <- t:
	case <-ctx.Done():
		//超时了返回
		return ctx.Err()
	}
	return nil
}

// Close 关闭任务池，原则是谁开的谁就关
func (p *TaskPool) Close() error {
	//p.close.Store(true)
	//这种写法不行，因为发送一个close的信号，只有一个goroutine会接收到，其他goroutine不会收到，
	//p.close <- struct{}{}

	//这种方式关闭，有一种缺陷：重复调Close方法会panic。
	//close(p.close)

	//这种方式关闭，不会panic，保护住  close，防止多次关闭。
	p.closeOnce.Do(func() {
		close(p.close)
	})
	return nil
}

// ConnectNats 连接NATS
func (ns *Instance) ConnectNats(ins *Instance) (*Instance, error) {
	// 检查是否已存在连接
	if ns.NatServer != nil {
		if !ns.NatServer.IsClosed() {
			// 如果连接已经存在且未关闭，直接返回
			log.Println("ConnectNats0")
			return ns, nil
		}
	}

	if len(ins.NatsCluster) > 1 || (ins.NatsUser != "" && ins.NatsPassword != "") {
		nc, err := nats.Connect(strings.Join(ins.NatsCluster, ","),
			nats.UserInfo(ins.NatsUser, ins.NatsPassword),
			nats.Timeout(15*time.Second),
			nats.MaxReconnects(10),
			nats.ReconnectWait(15*time.Second),
			//nats.NoReconnect(),
		)
		if err != nil {
			log.Println("ConnectNats1", err, ins.NatsCluster, ins.NatsUser)
			ns.NatServer = nil
			return nil, err
		}
		if nc.Status().String() != "unknown status" || nc.Status().String() != "CLOSED" {
			ns.NatServer = nc
			log.Println(nc.Status().String())
			log.Println("ConnectNats2")
			return ns, nil
		}
		ns.NatServer = nil
		return nil, errors.New("e! ConnectNats2")
	}

	log.Println(ins.NatsCluster)
	server := ins.NatsCluster[0]
	if ins.NatsUser != "" && ins.NatsPassword != "" {
		nc, err := nats.Connect(server, nats.UserInfo(ns.NatsUser, ns.NatsPassword))
		if err != nil {
			ns.NatServer = nil
			return nil, err
		}
		if nc.Status().String() != "unknown status" || nc.Status().String() != "CLOSED" {
			ns.NatServer = nc
			log.Println("ConnectNats3")
			return ns, nil
		}
		ns.NatServer = nil
		return nil, errors.New("e! ConnectNats3")
	}
	nc, err := nats.Connect(server)
	if err != nil {
		ns.NatServer = nil
		return ns, err
	}

	if nc.Status().String() != "unknown status" || nc.Status().String() != "CLOSED" {
		ns.NatServer = nc
		log.Println("ConnectNats4")
		return ns, nil
	}
	ns.NatServer = nil
	return nil, errors.New("e! ConnectNats5")
}

// GetListStreams 兼容NATS集群，增加自定义指标：
// nats_healthz                # NATS集群健康状态；
// nats_msg_total              # 消息积压数量；
// nats_stream_msg_count       # Streams 对应消息数量；
// nats_stream_count_consumer  # Streams 消费者数量；
// nats_stream_consumer_count  # Streams 对应消费者数量；
// nats_streams_total          # Streams数量；
// nats_consumers_total        # Consumers数量，消息积压情况；
// 理解差异: 再次强调，核心NATS的客户端统计是流量概念，表征活动的活跃程度；而JetStream的Stream状态是存量概念，表征数据的积压情况。
func (ns *Instance) GetListStreams(ins *Instance, lst *types.SampleList) error {
	ins.StreamsTotal.Store(0)
	ins.ConsumersTotal.Store(0)
	ins.MsgTotal.Store(0)

	if ns.NatServer == nil {
		log.Println("E! ns.Nats connection is nil")
		return types.ErrInstancesEmpty
	}
	// 检查连接状态
	if ns.NatServer.IsClosed() {
		log.Println("E! ns.Nats connection is closed")
		ns.NatServer = nil
		return types.ErrInstancesEmpty
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	js, err := jetstream.New(ns.NatServer)
	if err != nil {
		log.Println("E! jetstream.New ", err)
		ns.NatServer.Close()
		ns.NatServer = nil
		return types.ErrInstancesEmpty
	}
	streams := js.ListStreams(ctx)
	if streams.Err() != nil {
		log.Println("Unexpected error occurred in stream list:", streams.Err())
		//return streams.Err()
		ns.NatServer.Close()
		ns.NatServer = nil
		return types.ErrInstancesEmpty
	}

	for s := range streams.Info() {
		if s == nil && s.Config.Name == "" {
			log.Println("streams.Info() nil##########")
			continue
		} else {
			//nats_stream_msg_count
			//lst.PushSamples(inputName, ins.NatsFields, ins.NatsTags)
			lst.PushSamples(inputName,
				map[string]interface{}{
					"stream_msg_count":      s.State.Msgs,
					"stream_count_consumer": s.State.Consumers,
				},
				map[string]string{
					"server": ins.Server,
					//"instance":    ns.NatServer.Servers()[0],
					"instance":    ins.NatsCluster[0],
					"stream_name": s.Config.Name,
				})
			if s.State.Consumers > 0 {
				ins.StreamsTotal.Add(int64(s.State.Consumers))
			}

			stream, err1 := js.Stream(ctx, s.Config.Name)
			if err1 != nil {
				log.Println("E! jetstream.Stream ", err1)
				continue // 继续处理其他流而不是直接返回错误
				//return types.ErrInstancesEmpty
			}
			consumers := stream.ListConsumers(ctx)
			// Returns the most recently fetched StreamInfo, without making an API call to the server
			if consumers.Err() != nil {
				log.Println("Unexpected error occurred in consumer list:", consumers.Err())
				continue
				//return types.ErrInstancesEmpty
			}
			info, err2 := stream.Info(ctx)
			if err2 != nil {
				log.Println("E! c.Info failed:", err2)
				continue
				//return types.ErrInstancesEmpty
			}

			//nats_stream_consumer_count
			//lst.PushSamples(inputName, ins.NatsFields, ins.NatsTags)
			lst.PushSamples(inputName,
				map[string]interface{}{
					"stream_consumer_msg_count": info.State.Msgs,
					"stream_consumer_count":     info.State.Consumers,
				},
				map[string]string{
					"server": ins.Server,
					//"instance":    ns.NatServer.Servers()[0],
					"instance":    ins.NatsCluster[0],
					"stream_name": info.Config.Name,
				})
			if info.State.Consumers > 0 {
				ins.ConsumersTotal.Add(int64(info.State.Consumers))
			}

			if s.State.Msgs > 0 && s.Config.Name != "" && info.Config.Name != "" {
				ins.MsgTotal.Add(int64(s.State.Msgs))
				log.Println("ins.MsgTotal.Add(int64(s.State.Msgs))", ins.MsgTotal.Load())
			}

			//nats_msg_total
			//lst.PushSamples(inputName, ins.NatsFields, ins.NatsTags)
			lst.PushSamples(inputName,
				map[string]interface{}{
					"msg_total": ins.MsgTotal.Load(),
				},
				map[string]string{
					"server": ins.Server,
					//"instance":    ns.NatServer.Servers()[0],
					"instance": ins.NatsCluster[0],
				})
			//log.Println("ins.NatsFields, ins.NatsTags", ins.NatsFields, ins.NatsTags)
		}
		time.Sleep(time.Millisecond * 10)
	}

	time.Sleep(time.Millisecond * 100)
	//nats_streams_total
	//lst.PushSamples(inputName, fields, tags)
	lst.PushSamples(inputName,
		map[string]interface{}{
			"streams_total": ins.StreamsTotal.Load(),
		},
		map[string]string{
			"server": ins.Server,
			//"instance": ns.NatServer.Servers()[0],
			"instance": ins.NatsCluster[0],
		})

	//nats_consumers_total
	//lst.PushSamples(inputName, fields, tags)
	lst.PushSamples(inputName,
		map[string]interface{}{
			"consumers_total": ins.ConsumersTotal.Load(),
		},
		map[string]string{
			"server": ins.Server,
			//"instance": ns.NatServer.Servers()[0],
			"instance": ins.NatsCluster[0],
		})

	return nil
}

// SetNatsHealthz 0表示异常，1表示正常
func (ns *Instance) SetNatsHealthz(lst *types.SampleList, ins *Instance) error {
	//log.Println("ns.NatsFields, ns.NatsTags", ins.NatsFields, ins.NatsTags, tagsFalse, fieldsFalse)
	if ns.NatServer == nil {
		log.Println("E! ns.Nats connection is nil")
		//lst.PushSamples(inputName, ns.NatsFields, ns.NatsTags)
		lst.PushSamples(inputName,
			map[string]interface{}{
				"healthz": 0,
			},
			map[string]string{
				"healthz":  "false",
				"instance": ins.NatsCluster[0],
			})
		ns.CloseJetStreams()
		return types.ErrInstancesEmpty
	}

	//HTTP获取NATS集群健康状态
	address, err := url.Parse(ins.Server)
	if err != nil {
		log.Println("E! error parseURL", err)
		//0表示异常，1表示正常
		//lst.PushSamples(inputName, ns.NatsFields, ns.NatsTags)
		lst.PushSamples(inputName,
			map[string]interface{}{
				"healthz": 0,
			},
			map[string]string{
				"healthz":  "false",
				"instance": ins.NatsCluster[0],
			})
		ns.CloseJetStreams()
		return types.ErrInstancesEmpty
	}
	address.Path = path.Join(address.Path, "healthz")
	conn, err := ins.client.Get(address.String())
	if err != nil {
		log.Println("E! error while polling", address.String(), err)
		lst.PushSamples(inputName,
			map[string]interface{}{
				"healthz": 0,
			},
			map[string]string{
				"healthz":  "false",
				"instance": ins.NatsCluster[0],
			})
		ns.CloseJetStreams()
		return types.ErrInstancesEmpty
	}
	body, err := io.ReadAll(conn.Body)
	if err != nil || len(body) == 0 {
		log.Println("E! error reading body", err)
		lst.PushSamples(inputName,
			map[string]interface{}{
				"healthz": 0,
			},
			map[string]string{
				"healthz":  "false",
				"instance": ins.NatsCluster[0],
			})
		body = nil
		ns.CloseJetStreams()
		return types.ErrInstancesEmpty
	}
	if string(body) != "{\"status\":\"ok\"}" {
		log.Println(string(body))
		lst.PushSamples(inputName,
			map[string]interface{}{
				"healthz": 0,
			},
			map[string]string{
				"healthz":  "false",
				"instance": ins.NatsCluster[0],
			})
		body = nil
		ns.CloseJetStreams()
		return types.ErrInstancesEmpty
	}
	status := new(gnatsd.HealthStatus)
	err = json.Unmarshal(body, &status)
	//log.Println(status.Status, status.StatusCode, status.Errors)
	if err != nil || status.Errors != nil {
		log.Println("E! error parsing response", err)
		lst.PushSamples(inputName,
			map[string]interface{}{
				"healthz": 0,
			},
			map[string]string{
				"healthz":  "false",
				"instance": ins.NatsCluster[0],
			})
		ns.CloseJetStreams()
		return types.ErrInstancesEmpty
	}
	//0表示异常，1表示正常
	//lst.PushSamples(inputName, ns.NatsFields, ns.NatsTags)
	lst.PushSamples(inputName,
		map[string]interface{}{
			"healthz": 1,
		},
		map[string]string{
			"healthz":  "true",
			"instance": ns.NatServer.Servers()[0],
		})

	err = conn.Body.Close()
	if err != nil {
		log.Println("E! error closing body", err)
	}

	return nil
}

func (ns *Instance) CloseJetStreams() {
	if ns.NatServer != nil {
		//nc := ns.NatServer
		//nc.Close()
		ns.NatServer.Close()
		ns.NatServer = nil // 防止重复关闭
	}
}

func (ns *Instance) getConfigPath() (string, error) {
	fileName := "nats.toml"
	fileDirUnix := "/conf/input.nats/"
	fileDirWindows := "\\conf\\input.nats\\"
	dir, err := os.Getwd()
	if err != nil {
		log.Println("无法打开配置文件")
		return "nil", types.ErrInstancesEmpty
	}
	switch runtime.GOOS {
	case "linux":
		log.Println("运行在Linux系统上", dir+fileDirUnix+fileName)
		return fmt.Sprintf(dir + fileDirUnix + fileName), nil
	case "darwin":
		log.Println("运行在macOS系统上", dir+fileDirUnix+fileName)
		return fmt.Sprintf(dir + fileDirUnix + fileName), nil
	case "windows":
		log.Println("运行在Windows系统上", dir+fileDirWindows+fileName)
		return fmt.Sprintf(dir + fileDirWindows + fileName), nil
	default:
		log.Println("当前系统不支持")
		return "nil", types.ErrInstancesEmpty
	}
}

func (ns *Instance) initConfig(ins *Instance, f string) error {
	_, err := os.OpenFile(f, os.O_RDONLY, 0444)
	if err != nil {
		log.Println("无法打开配置文件")
		return types.ErrInstancesEmpty
	}
	if file, err := toml.DecodeFile(f, &ins); err != nil {
		log.Println("配置文件初始化出错", file)
		return types.ErrInstancesEmpty
	}
	//file, err := toml.DecodeFile(f, &ins)
	//file.Keys()
	//fmt.Println(file, err)
	//log.Println("加载配置文件成功", openFile)
	return nil
}
