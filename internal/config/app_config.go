package config

import (
	"time"
)

type AppConfig struct {
	Port           int           `yaml:"port"`
	DefaultTimeout time.Duration `yaml:"default_timeout"`
}
