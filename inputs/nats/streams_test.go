package nats

// https://github.com/nats-io/nats.go/blob/main/jetstream/README.md

import (
	"context"
	"fmt"
	NatsServer "github.com/nats-io/nats-server/v2/server"
	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"log"
	"strings"
	"testing"
	"time"
)

func TestFields(t *testing.T) {
	servers := []string{"nats://10.0.1.249:4221", "nats://10.0.1.249:4222", "nats://10.0.1.249:4223"}
	//servers := []string{"nats://10.0.1.5:4222", "nats://10.0.1.6:4222", "nats://10.0.1.7:4222"}
	//nc, _ := nats.Connect("nats://10.0.1.249:4223", nats.UserInfo("nats-client-user-test", "nxh0oJtPW8l6gv2YfjOk"))
	//nc, _ := nats.Connect("demo.nats.io")
	nc, err := nats.Connect(strings.Join(servers, ","),
		nats.UserInfo("nats-client-user-test", "nxh0oJtPW8l6gv2YfjOk"),
		nats.Timeout(10*time.Second),
		nats.MaxReconnects(10),
		nats.ReconnectWait(15*time.Second),
		//nats.NoReconnect(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	//ticker := time.NewTicker(3 * time.Second)
	//defer ticker.Stop()
	//for range ticker.C {
	//	fmt.Println("ticker.C: ", time.Now())
	//	stats := nc.Stats()
	//	log.Printf("Messages: In=%d, Out=%d, Reconnects=%d",
	//		stats.InMsgs, stats.OutMsgs, stats.Reconnects)
	//}

	// In the `jetstream` package, almost all API calls rely on `context.Context` for timeout/cancellation handling
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Create a Instance management interface
	js, _ := jetstream.New(nc)

	//stream, _ := js.Stream(ctx, "ORDERS")
	//info, _ := stream.Info(ctx)
	//log.Printf("Stream: Messages=%d, Bytes=%d, Consumers=%d",
	//	info.State.Msgs, info.State.Bytes, info.State.Consumers)

	// Do something with the connection
	// 获取最大有效负载大小 https://docs.nats.io/using-nats/developer/connecting/misc
	mp := nc.MaxPayload()
	log.Printf("Maximum payload is %v bytes", mp)

	fmt.Println("nc.Stats()============")
	fmt.Println(NatsServer.HealthStatus{}.Status)
	fmt.Println(nc.Status())
	fmt.Println(nc.Status().String())
	fmt.Println(nc.Stats())

	// list streams
	streams := js.ListStreams(ctx)
	for s := range streams.Info() {
		fmt.Println("range streams.Info():")
		fmt.Println(s.State.Msgs)
		fmt.Println(s.State.Consumers)
		fmt.Println(s.Config.Name)

		//s, _ := js.Stream(ctx, "camera_video_plan_record_duration_reply")
		c, _ := js.Stream(ctx, s.Config.Name)
		consumers := c.ListConsumers(ctx)
		// Fetches latest stream info from server
		info, _ := c.Info(ctx)
		fmt.Println("########################")
		fmt.Println(info)
		fmt.Println(info.Config.Name)
		fmt.Println(info.State.Msgs)
		fmt.Println(info.State.Consumers)
		// Returns the most recently fetched StreamInfo, without making an API call to the server
		cachedInfo := c.CachedInfo()
		fmt.Println(cachedInfo.State)
		for cons := range consumers.Info() {
			fmt.Println("camera_video_plan_record_duration_reply########################")
			fmt.Println(cons)
			fmt.Println(cons.Config.Name)
		}
		if consumers.Err() != nil {
			fmt.Println("Unexpected error occurred")
		}

	}
	if streams.Err() != nil {
		fmt.Println("Unexpected error occurred")
	}

	// list stream names
	names := js.StreamNames(ctx)
	for name := range names.Name() {
		fmt.Println("js.StreamNames(ctx): ", name)
	}
	if names.Err() != nil {
		fmt.Println("Unexpected error occurred")
	}

	// 发布消息到主题 "foo"
	subject := "foo"
	message := []byte("Hello, NATS! 123")
	err = nc.Publish(subject, message)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("已发布消息到主题 '%s': %s\n", subject, string(message))

	//cancel()
	// Close the connection
	nc.Close()
}

func TestSubscribe(t *testing.T) {
	servers := []string{"nats://10.0.1.249:4221", "nats://10.0.1.249:4222", "nats://10.0.1.249:4223"}
	nc, err := nats.Connect(strings.Join(servers, ","),
		nats.UserInfo("nats-client-user-test", "nxh0oJtPW8l6gv2YfjOk"),
		nats.Timeout(10*time.Second), nats.MaxReconnects(10), nats.ReconnectWait(15*time.Second),
		//nats.NoReconnect(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()
	// In the `jetstream` package, almost all API calls rely on `context.Context` for timeout/cancellation handling
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Create a Instance management interface
	js, _ := jetstream.New(nc)

	// Do something with the connection
	// 获取最大有效负载大小 https://docs.nats.io/using-nats/developer/connecting/misc
	mp := nc.MaxPayload()
	log.Printf("Maximum payload is %v bytes", mp)

	fmt.Println("nc.Stats()============")
	fmt.Println(NatsServer.HealthStatus{}.Status)
	fmt.Println(nc.Status())
	fmt.Println(nc.Status().String())
	fmt.Println(nc.Stats())

	// list streams
	streams := js.ListStreams(ctx)
	for s := range streams.Info() {
		fmt.Println("range streams.Info():")
		fmt.Println(s.State.Msgs)
		fmt.Println(s.State.Consumers)
		fmt.Println(s.Config.Name)

		//s, _ := js.Stream(ctx, "camera_video_plan_record_duration_reply")
		c, _ := js.Stream(ctx, s.Config.Name)
		consumers := c.ListConsumers(ctx)
		// Fetches latest stream info from server
		info, _ := c.Info(ctx)
		fmt.Println("########################")
		fmt.Println(info)
		fmt.Println(info.Config.Name)
		fmt.Println(info.State.Msgs)
		fmt.Println(info.State.Consumers)
		// Returns the most recently fetched StreamInfo, without making an API call to the server
		cachedInfo := c.CachedInfo()
		fmt.Println(cachedInfo.State)
		for cons := range consumers.Info() {
			fmt.Println("camera_video_plan_record_duration_reply########################")
			fmt.Println(cons)
			fmt.Println(cons.Config.Name)
		}
		if consumers.Err() != nil {
			fmt.Println("Unexpected error occurred")
		}

	}
	if streams.Err() != nil {
		fmt.Println("Unexpected error occurred")
	}

	// list stream names
	names := js.StreamNames(ctx)
	for name := range names.Name() {
		fmt.Println("js.StreamNames(ctx): ", name)
	}
	if names.Err() != nil {
		fmt.Println("Unexpected error occurred")
	}

	//消费者 (Subscriber)：订阅特定主题并处理消息。支持异步和同步方式。
	//异步订阅（推荐：使用回调函数非阻塞处理。
	nc.Subscribe("foo", func(m *nats.Msg) {
		log.Printf("收到消息: %s\n", string(m.Data))
	})
	// 保持程序运行以持续接收消息
	select {}

	////同步订阅：阻塞等待下一条消息。
	//if err != nil {
	//	log.Fatal(err)
	//}
	//m, err := sub.NextMsg(10 * time.Second) // 等待10秒
	//if err != nil {
	//	log.Fatal(err)
	//}
	//log.Printf("收到消息: %s\n", string(m.Data))

}

func TestCreateStream(t *testing.T) {
	servers := []string{"nats://10.0.1.249:4221", "nats://10.0.1.249:4222", "nats://10.0.1.249:4223"}
	nc, err := nats.Connect(strings.Join(servers, ","),
		nats.UserInfo("nats-client-user-test", "nxh0oJtPW8l6gv2YfjOk"),
		nats.Timeout(10*time.Second), nats.MaxReconnects(10), nats.ReconnectWait(15*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()
	// In the `jetstream` package, almost all API calls rely on `context.Context` for timeout/cancellation handling
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Create a Instance management interface
	//js, err := jetstream.New(nc)
	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	// 定义流配置，例如流名为"ORDERS"，捕获所有以"orders.>"开头的主题
	streamCfg := &nats.StreamConfig{
		Name:      "ORDERS",
		Subjects:  []string{"orders.>"}, // 流的主题过滤器
		Retention: nats.LimitsPolicy,    // 保留策略
		MaxBytes:  100 * 1024 * 1024,    // 流的最大字节数
	}
	// 添加或更新流
	_, err = js.AddStream(streamCfg)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("nc.Stats()============")
	fmt.Println(NatsServer.HealthStatus{}.Status)
	fmt.Println(nc.Status())
	fmt.Println(nc.Status().String())
	fmt.Println(nc.Stats())
	fmt.Println(ctx.Err())

}

func TestPushStream(t *testing.T) {
	servers := []string{"nats://10.0.1.249:4221", "nats://10.0.1.249:4222", "nats://10.0.1.249:4223"}
	nc, err := nats.Connect(strings.Join(servers, ","),
		nats.UserInfo("nats-client-user-test", "nxh0oJtPW8l6gv2YfjOk"),
		nats.Timeout(10*time.Second), nats.MaxReconnects(10), nats.ReconnectWait(15*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Create a Instance management interface
	//js, err := jetstream.New(nc)
	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	// 发布消息到流"ORDERS"的主题"orders.new"
	ack, err := js.Publish("orders.new", []byte("Order Data"))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("消息已持久化，序列号: %d", ack.Sequence)

	fmt.Println("nc.Stats()============")
	fmt.Println(NatsServer.HealthStatus{}.Status)
	fmt.Println(nc.Status())
	fmt.Println(nc.Status().String())
	fmt.Println(nc.Stats())
	fmt.Println(ctx.Err())

}

//

// TestCreateStreamWorkQueue 创建流和消费者：首先确保基础设置就绪。
// 步骤1
func TestCreateStreamWorkQueue(t *testing.T) {
	servers := []string{"nats://10.0.1.249:4221", "nats://10.0.1.249:4222", "nats://10.0.1.249:4223"}
	//servers := []string{"nats://10.0.1.7:4222", "nats://10.0.1.6:4222", "nats://10.0.1.5:4222"}
	nc, err := nats.Connect(strings.Join(servers, ","),
		nats.UserInfo("nats-client-user-test", "nxh0oJtPW8l6gv2YfjOk"),
		nats.Timeout(10*time.Second), nats.MaxReconnects(10), nats.ReconnectWait(15*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	jets, _ := nc.JetStream()
	//获取消费者级别的信息，获取消费者信息（ConsumerInfo）是监控积压（NumPending）的关键。
	//consumer, err := js.ConsumerInfo("YOUR_STREAM_NAME", "YOUR_CONSUMER_NAME")
	// Create a Instance management interface
	//js, err := jetstream.New(nc)
	js, err := jetstream.New(nc)
	if err != nil {
		log.Println("log.Fatal(err)>>>")
		log.Fatal(err)
	}

	// 创建或获取流（以WorkQueue为例）
	cfg := jetstream.StreamConfig{
		Name:      "BACKLOG_STREAM",
		Subjects:  []string{"tasks.>"},
		Retention: jetstream.WorkQueuePolicy,
	}
	stream, err := js.CreateStream(ctx, cfg)

	// 创建拉取型消费者
	consumerx, err := jets.AddConsumer("BACKLOG_STREAM", &nats.ConsumerConfig{
		Durable:   "backlog-consumer",
		AckPolicy: nats.AckExplicitPolicy,
	})
	log.Println(consumerx, stream)

}

// TestCreateStreamWorkQueue 快速生产1000条消息
// 步骤2
func TestStreamWorkQueuePublish(t *testing.T) {
	servers := []string{"nats://10.0.1.249:4221", "nats://10.0.1.249:4222", "nats://10.0.1.249:4223"}
	//servers := []string{"nats://10.0.1.7:4222", "nats://10.0.1.6:4222", "nats://10.0.1.5:4222"}
	nc, err := nats.Connect(strings.Join(servers, ","),
		nats.UserInfo("nats-client-user-test", "nxh0oJtPW8l6gv2YfjOk"),
		nats.Timeout(10*time.Second), nats.MaxReconnects(10), nats.ReconnectWait(15*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Println("log.Fatal(err)>>>")
		log.Fatal(err)
	}

	// 快速生产100条消息
	go func() {
		for i := 1; i <= 100; i++ {
			msg := fmt.Sprintf("Task message #%d", i)
			if _, err := js.Publish(ctx, "tasks.us", []byte(msg)); err != nil {
				log.Fatalf("Failed to publish: %v", err)
			}
		}
		fmt.Println("100 messages published.")
		time.Sleep(time.Second * 3)
	}()

}

// TestStreamWorkQueuePullSubscribe
// 慢速或暂停消费：这是制造积压的关键。启动一个消费者，但让其处理速度极慢（例如每条消息处理几秒），或者在处理少量消息后直接暂停，不再继续拉取新消息。
// 步骤3
func TestStreamWorkQueuePullSubscribe(t *testing.T) {
	servers := []string{"nats://10.0.1.249:4221", "nats://10.0.1.249:4222", "nats://10.0.1.249:4223"}
	//servers := []string{"nats://10.0.1.7:4222", "nats://10.0.1.6:4222", "nats://10.0.1.5:4222"}
	nc, err := nats.Connect(strings.Join(servers, ","),
		nats.UserInfo("nats-client-user-test", "nxh0oJtPW8l6gv2YfjOk"),
		nats.Timeout(10*time.Second), nats.MaxReconnects(10), nats.ReconnectWait(15*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()
	jets, err := nc.JetStream()
	if err != nil {
		log.Println("log.Fatal(err)>>>")
		log.Fatal(err)
	}

	sub, _ := jets.PullSubscribe("tasks.>", "backlog-consumer")
	// 仅消费少量消息，然后让客户端挂起，不再拉取新消息
	msgs, _ := sub.Fetch(10, nats.MaxWait(2*time.Second))
	for _, msg := range msgs {
		// 模拟慢速处理
		time.Sleep(10 * time.Second)
		fmt.Printf("Slow processing: %s\n", string(msg.Data))
		msg.Ack() // 确认消息
	}
	// 在此之后，不再调用 Fetch，剩余消息将形成积压

}

// TestStreamWorkQueueConsumerInfo 等待片刻后查询消费者信息.
// 完成这些步骤后，您就可以通过检查消费者信息来观察积压了。
// 步骤4
func TestStreamWorkQueueConsumerInfo(t *testing.T) {
	servers := []string{"nats://10.0.1.249:4221", "nats://10.0.1.249:4222", "nats://10.0.1.249:4223"}
	//servers := []string{"nats://10.0.1.7:4222", "nats://10.0.1.6:4222", "nats://10.0.1.5:4222"}
	nc, err := nats.Connect(strings.Join(servers, ","),
		nats.UserInfo("nats-client-user-test", "nxh0oJtPW8l6gv2YfjOk"),
		nats.Timeout(10*time.Second), nats.MaxReconnects(10), nats.ReconnectWait(15*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()
	jets, err := nc.JetStream()
	if err != nil {
		log.Println("log.Fatal(err)>>>")
		log.Fatal(err)
	}

	// 等待片刻后查询消费者信息
	info, err := jets.ConsumerInfo("BACKLOG_STREAM", "backlog-consumer")
	if err != nil {
		log.Fatal(err)
	} else {
		// 关键指标：NumPending 表示待处理的消息数，即积压量
		log.Printf("Current backlog (NumPending): %d messages\n", info.NumPending)
		log.Printf("Unacknowledged messages (NumAckPending): %d\n", info.NumAckPending)
	}

}
