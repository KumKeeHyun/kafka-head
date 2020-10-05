package helper

import (
	"strings"

	"github.com/Shopify/sarama"
	"github.com/spf13/viper"
)

func init() {
	viper.SetEnvPrefix("kafka")
	viper.BindEnv("brokers")
	viper.BindEnv("topic")
	viper.BindEnv("version")

	viper.SetDefault("brokers", []string{"220.70.2.5:8082"})
	viper.SetDefault("topic", "__consumer_offsets")
	viper.SetDefault("version", "2.4.0")
}

func SplitStringSlice(raw []string) (res []string) {
	for _, v := range raw {
		res = append(res, strings.Split(v, ",")...)
	}
	return
}

var KafkaVersions = map[string]sarama.KafkaVersion{
	"":         sarama.V0_10_2_0,
	"0.8.0":    sarama.V0_8_2_0,
	"0.8.1":    sarama.V0_8_2_1,
	"0.8.2":    sarama.V0_8_2_2,
	"0.8":      sarama.V0_8_2_0,
	"0.9.0.0":  sarama.V0_9_0_0,
	"0.9.0.1":  sarama.V0_9_0_1,
	"0.9.0":    sarama.V0_9_0_0,
	"0.9":      sarama.V0_9_0_0,
	"0.10.0.0": sarama.V0_10_0_0,
	"0.10.0.1": sarama.V0_10_0_1,
	"0.10.0":   sarama.V0_10_0_0,
	"0.10.1.0": sarama.V0_10_1_0,
	"0.10.1":   sarama.V0_10_1_0,
	"0.10.2.0": sarama.V0_10_2_0,
	"0.10.2.1": sarama.V0_10_2_0,
	"0.10.2":   sarama.V0_10_2_0,
	"0.10":     sarama.V0_10_0_0,
	"0.11.0.1": sarama.V0_11_0_0,
	"0.11.0.2": sarama.V0_11_0_0,
	"0.11.0":   sarama.V0_11_0_0,
	"1.0.0":    sarama.V1_0_0_0,
	"1.1.0":    sarama.V1_1_0_0,
	"1.1.1":    sarama.V1_1_0_0,
	"2.0.0":    sarama.V2_0_0_0,
	"2.0.1":    sarama.V2_0_0_0,
	"2.1.0":    sarama.V2_1_0_0,
	"2.2.0":    sarama.V2_2_0_0,
	"2.2.1":    sarama.V2_2_0_0,
	"2.3.0":    sarama.V2_3_0_0,
	"2.4.0":    sarama.V2_4_0_0,
	"2.5.0":    sarama.V2_5_0_0,
	"2.6.0":    sarama.V2_6_0_0,
}
