package udf

import "time"

type Config struct {
	Functions map[string]FunctionConfig
}

type FunctionConfig struct {
	Prog    string
	Args    []string
	Timeout time.Duration
}

func NewConfig() Config {
	return Config{}
}
