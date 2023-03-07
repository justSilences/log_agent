package main

import (
	"logagent/kafka"
	"logagent/tailfile"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
)

func run() (err error) {
	for {
		line, ok := <-tailfile.TailObj.Lines
		if !ok {
			time.Sleep(time.Second)
			continue
		}
		logrus.Info(line.Text)

		// 如果是空行略过
		if len(strings.Trim(line.Text, "\r")) == 0 {
			continue
		}

		// 3. 构造结构体
		msg := &sarama.ProducerMessage{}
		msg.Topic = "web_log"
		msg.Value = sarama.StringEncoder(line.Text)

		// 将数据丢到创建的MsgChan中
		kafka.ToMsgChan(msg)
	}

}

// 收集指定目录下的日志文件，发送到kafka
func main() {

	// 0.增加配置文件，加载配置文件
	cfg, err := ini.Load("./conf/config.ini")
	if err != nil {
		// fmt.Println("ini load ERROR", err)
		logrus.Error("ini load ERROR", err)
		return
	}
	// 01读配置
	kafkaAddr := cfg.Section("kafka").Key("address").String()
	kafkaChanSize, _ := cfg.Section("kafka").Key("chan_size").Int64()

	// 1.初始化（做好准备工作）
	// 01.初始化kafka
	err = kafka.Init([]string{kafkaAddr}, kafkaChanSize)
	if err != nil {
		logrus.Error("init kafka ERROR:", kafkaAddr, err)
		return
	}
	logrus.Info("init kafka SUCCESS!")
	// 02.初始化tail包
	logfile := cfg.Section("collect").Key("logfile_path").String()
	err = tailfile.Init(logfile)
	if err != nil {
		logrus.Error("init logfile ERROR:", logfile, err)
		return
	}
	logrus.Info("init logfile SUCCESS!")

	// 2.根据配置文件中的日志路径使用tail去收集日志
	// tail -> kafka
	err = run()
	if err != nil {
		logrus.Error("run ERROR:", err)
	}

	// 3.把日志通过sarama发往kafka
}
