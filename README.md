### categraf
categraf-v0.4.5 ```https://github.com/flashcatcloud/categraf.git```

categraf官方不支持NATS集群监控且缺少消息积压监控指标，所以这里添加了NATS集群监控并且添加了一些自定义指标。

#### 自定义指标
```
// nats_healthz                # NATS集群健康状态
// nats_msg_total              # 消息积压数量(核心)
// nats_stream_msg_count       # Streams 对应消息数量
// nats_stream_count_consumer  # Streams 消费者数量
// nats_stream_consumer_count  # Streams 对应消费者数量
// nats_streams_total          # Streams数量
// nats_consumers_total        # Consumers数量，消息积压情况

// 理解差异: 要强调的是，核心NATS的客户端统计是流量概念，表征活动的活跃程度；
而JetStream的Stream状态是存量概念，表征数据的积压情况。
```

#### dashboards仪表盘
NATS-Server-Dashboard.json
<p align="center">
  <img src="https://raw.githubusercontent.com/linuxsun/categraf/refs/heads/main/images/NATS-dashboard-1.png"/>
</p>

nats-Dashboard.json
<p align="center">
  <img src="https://raw.githubusercontent.com/linuxsun/categraf/refs/heads/main/images/NATS-dashboard-2.png"/>
</p>
