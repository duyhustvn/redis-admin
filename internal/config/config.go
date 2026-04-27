// Package config loads and validates service configuration.
package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config holds all service configuration values.
type Config struct {
	SentinelAddrs    []string      `mapstructure:"sentinel_addrs"`
	MasterName       string        `mapstructure:"master_name"`
	SentinelPassword string        `mapstructure:"sentinel_password"`
	RedisPassword    string        `mapstructure:"redis_password"`
	K8sNamespace     string        `mapstructure:"k8s_namespace"`
	LogLevel         string        `mapstructure:"log_level"`
	HTTPPort         int           `mapstructure:"http_port"`
	MetricsPort      int           `mapstructure:"metrics_port"`
	PollInterval     time.Duration `mapstructure:"poll_interval"`
	SlowlogInterval  time.Duration `mapstructure:"slowlog_interval"`
	WebhookURL       string        `mapstructure:"webhook_url"`
}

// Load reads configuration from the YAML file at path and then apply
func Load() (*Config, error) {
	v := viper.New()

	v.SetDefault("sentinel_addrs", []string{"localhost:26379"})
	v.SetDefault("master_name", "mymaster")
	v.SetDefault("k8s_namespace", "default")
	v.SetDefault("log_level", "info")
	v.SetDefault("http_port", 8080)
	v.SetDefault("metrics_port", 9090)
	v.SetDefault("poll_interval", "5s")
	v.SetDefault("slowlog_interval", "10s")

	// config read from yaml
	v.AddConfigPath(".") // search at this directory
	v.SetConfigName("config")
	v.SetConfigType("yaml")

	// Config read from env
	v.AutomaticEnv()

	// Change "." in struct to "_" in ENV (server.port -> SERVER_PORT)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			// error is not "file not found"
			return nil, err
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return &cfg, nil
}

func (c *Config) validate() error {
	if len(c.SentinelAddrs) == 0 {
		return fmt.Errorf("sentinel_addrs must not be empty")
	}
	if c.MasterName == "" {
		return fmt.Errorf("master_name must not be empty")
	}
	if c.HTTPPort <= 0 || c.HTTPPort > 65535 {
		return fmt.Errorf("http_port %d is out of range", c.HTTPPort)
	}
	return nil
}
