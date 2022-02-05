package kafkaexporter

import (
	"github.com/Shopify/sarama"
	"go.uber.org/zap"
)

type (
	SaramaLogger struct {
		logger *zap.SugaredLogger
	}
)

var (
	_ sarama.StdLogger = &SaramaLogger{}
)

func NewSaramaLogger(logger *zap.Logger) *SaramaLogger {
	return &SaramaLogger{
		logger: logger.Named("sarama").Sugar(),
	}
}

func (sl *SaramaLogger) Print(v ...interface{}) {
	sl.logger.Debug(v...)
}

func (sl *SaramaLogger) Printf(format string, v ...interface{}) {
	sl.logger.Debugf(format, v...)
}

func (sl *SaramaLogger) Println(v ...interface{}) {
	sl.logger.Debug(append(v, "\n")...)
}
