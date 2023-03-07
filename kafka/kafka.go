package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

// kafka相关操作

var (
	client  sarama.SyncProducer
	msgChan chan *sarama.ProducerMessage
)

// 初始化全局的kafka连接  Client
func Init(address []string, chanSize int64) (err error) {
	// 1. 生产者配置  获取config对象，
	config := sarama.NewConfig()

	// 设置 生产者 config
	// 设置发送完数据后是否需要确定：   需要leader和follow都确定
	config.Producer.RequiredAcks = sarama.WaitForAll

	//设置数据发送的分区
	config.Producer.Partitioner = sarama.NewRandomPartitioner

	// 成功交付的消息将在success channel返回
	config.Producer.Return.Successes = true

	// 2. 根据配置 连接kafka
	client, err = sarama.NewSyncProducer(address, config)
	if err != nil {
		logrus.Error("Kafka:NewSyncProducer ERROR:", err)
		return
	}
	// 初始化MsgChan
	// TODO add ini
	msgChan = make(chan *sarama.ProducerMessage, chanSize)
	// 启动goroutine从MsgChan中读数据并发送到kafka
	go SendMsg()
	return
}

// 发送数据
func SendMsg() {
	for {
		select {
		case msg := <-msgChan:
			pid, offset, err := client.SendMessage(msg)
			if err != nil {
				logrus.Error("Kafka:SendMessage ERROR:", err)
				return
			}
			logrus.Infof("Kafka send message success pid%v, offset%v", pid, offset)

		}
	}

}

// 避免对外暴漏msgChan 封装一个方法
func ToMsgChan(msg *sarama.ProducerMessage) {
	msgChan <- msg
}
