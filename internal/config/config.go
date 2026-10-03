package config

import (
	"os"
	"strconv"
)

type Config struct {
	Port          string
	DatabaseURL   string
	RabbitMQURL   string
	InternalToken string
	DBMaxConns    int32
}

func Load() Config {
	return Config{
		Port:          valueOrDefault("PORT", "8080"),
		DatabaseURL:   valueOrDefault("DATABASE_URL", "postgres://orchestrator:orchestrator@localhost:5432/orchestrator?sslmode=disable"),
		RabbitMQURL:   valueOrDefault("RABBITMQ_URL", "amqp://orchestrator:orchestrator@localhost:5672/"),
		InternalToken: valueOrDefault("INTERNAL_TOKEN", "local-development-token"),
		DBMaxConns:    int32(intValueOrDefault("DB_MAX_CONNS", 50)),
	}
}

func valueOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func intValueOrDefault(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil || value < 1 {
		return fallback
	}
	return value
}
