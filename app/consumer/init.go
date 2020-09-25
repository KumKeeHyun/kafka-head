package consumer

import "github.com/spf13/viper"

func init() {
	viper.SetEnvPrefix("kafka")
	viper.BindEnv("brokers")
	viper.BindEnv("topic")
	viper.BindEnv("version")

	viper.SetDefault("brokers", []string{"220.70.2.5:8082"})
	viper.SetDefault("topic", "cluster-test")
	viper.SetDefault("version", "2.4.0")
}
