package config

import (
	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Postgres Postgres   `yaml:"POSTGRES"`
	HTTP     HTTPServer `yaml:"HTTP"`
	RabbitMQ RabbitMQ   `yaml:"RABBITMQ"`
	Telegram Telegram   `yaml:"TELEGRAM"`
	SMTP     SMTP       `yaml:"EMAIL"`
}

type Postgres struct {
	Host        string `yaml:"POSTGRES_HOST" default:"localhost"`
	Port        uint16 `yaml:"POSTGRES_PORT" default:"5432"`
	User        string `yaml:"POSTGRES_USER" default:"postgres"`
	Password    string `yaml:"POSTGRES_PASSWORD" default:"postgres"`
	Database    string `yaml:"POSTGRES_DB" default:"postgres"`
	MaxConns    int32  `yaml:"MAX_CONNS" default:"10"`
	MinConns    int32  `yaml:"MIN_CONNS" default:"5"`
	PostgresDSN string `yaml:"POSTGRES_DSN"`
}

type HTTPServer struct {
	Address string `yaml:"ADDRESS" default:"localhost:8080"`
}

type RabbitMQ struct {
	URL            string `yaml:"URL" default:"amqp://guest:guest@localhost:5672/"`
	ConnectionName string `yaml:"CONNECTION_NAME" default:"DelayedNotifier"`
	ExchangeName   string `yaml:"EXCHANGE" default:"DelayedNotifier"`
}

type Telegram struct {
	Token string `yaml:"BOT_TOKEN"`
}

type SMTP struct {
	SMTPHost    string `yaml:"SMTP_HOST"`
	SMTPPort    string `yaml:"SMTP_PORT"`
	SenderEmail string `yaml:"SENDER_EMAIL"`
	Password    string `yaml:"PASSWORD"`
}

func LoadConfig(path string) (*Config, error) {
	var cfg Config
	if err := cleanenv.ReadConfig(path, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
