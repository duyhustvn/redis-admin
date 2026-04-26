// Package config loads and validates service configuration.
package config

import (
	"fmt"
	"os"
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

// Load reads configuration from the YAML file at path and then applies
// any RSA_* environment variable overrides.
func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetEnvPrefix("RSA")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	v.SetDefault("sentinel_addrs", []string{"localhost:26379"})
	v.SetDefault("master_name", "mymaster")
	v.SetDefault("k8s_namespace", "default")
	v.SetDefault("log_level", "info")
	v.SetDefault("http_port", 8080)
	v.SetDefault("metrics_port", 9090)
	v.SetDefault("poll_interval", "5s")
	v.SetDefault("slowlog_interval", "10s")

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config file %s: %w", path, err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	// Viper does not automatically split comma-separated env vars into slices.
	if s := os.Getenv("RSA_SENTINEL_ADDRS"); s != "" {
		cfg.SentinelAddrs = strings.Split(s, ",")
	}
	if s := os.Getenv("RSA_SENTINEL_PASSWORD"); s != "" {
		cfg.SentinelPassword = s
	}
	if s := os.Getenv("RSA_REDIS_PASSWORD"); s != "" {
		cfg.RedisPassword = s
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
