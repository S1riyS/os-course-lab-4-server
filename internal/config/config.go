package config

import (
	"os"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	App      AppConfig      `yaml:"app"`
	Database DatabaseConfig `yaml:"database"`
}

func MustLoad(configPath string) *Config {
	if configPath == "" {
		panic("config path is empty")
	}

	// check if file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		panic("config file does not exist: " + configPath)
	}

	// Read config file
	data, err := os.ReadFile(configPath)
	if err != nil {
		panic("failed to read data from config file: " + configPath)
	}

	// Enrich with env variables
	data = expandEnvVars(data)

	// Serialize to struct
	var cfg Config
	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		panic("cannot read config: " + err.Error())
	}

	return &cfg
}

func expandEnvVars(data []byte) []byte {
	return []byte(os.ExpandEnv(string(data)))
}
