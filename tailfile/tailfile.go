package tailfile

import (
	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
)

var (
	TailObj *tail.Tail
)

func Init(filename string) (err error) {

	config := tail.Config{
		ReOpen:    true,                                 // 日志文件到一定大小会自动切割，自动打开
		Follow:    true,                                 // 分割后跟随文件名
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2}, // 从文件末尾去读
		MustExist: false,                                // 文件可以不存在
		Poll:      true,                                 // 轮循方式去读
	}

	// 打开文件
	TailObj, err = tail.TailFile(filename, config)
	if err != nil {
		logrus.Error("tailfile ERR", err)
		return
	}
	return
}
