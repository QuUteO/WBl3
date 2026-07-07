package main

import (
	"DelayedNotifier/internal/config"
	"DelayedNotifier/internal/handler"
	"DelayedNotifier/internal/rabbitmq"
	"DelayedNotifier/internal/repository"
	"DelayedNotifier/internal/service"
	"fmt"
	"os"
	"time"

	"github.com/wb-go/wbf/dbpg/pgx-driver"
	"github.com/wb-go/wbf/ginext"
	"github.com/wb-go/wbf/logger"
	"github.com/wb-go/wbf/rabbitmq"
	"github.com/wb-go/wbf/retry"
)

func main() {
	cfg, err := config.LoadConfig("/Users/mihailignatev/Desktop/WBl3/l3.1/config.yaml")
	if err != nil {
		fmt.Println(err)
	}

	log, err := logger.InitLogger(
		logger.ZapEngine,
		"",
		"local",
		logger.WithLevel(logger.InfoLevel),
		logger.WithRotation("logs/app.log", 100, 5, 30),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Ошибка инициализации логгера: %v\n", err)
		os.Exit(1)
	}

	pgx, err := pgxdriver.New(
		cfg.Postgres.PostgresDSN,
		log,
		pgxdriver.MaxPoolSize(50),
		pgxdriver.MaxConnAttempts(5),
		pgxdriver.BaseRetryDelay(100*time.Millisecond),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Ошибка подключения к базе данных: %v\n ", err)
		os.Exit(1)
	}
	log.Info("База данных запустилась")

	strategy := retry.Strategy{
		Attempts: 5,
		Delay:    3 * time.Second,
		Backoff:  2,
	}

	rabbitCfg := rabbitmq.ClientConfig{
		URL:            cfg.RabbitMQ.URL,
		ConnectionName: cfg.RabbitMQ.ConnectionName,
		ConnectTimeout: 5 * time.Second,
		Heartbeat:      10 * time.Second,
		ProducingStrat: strategy,
		ConsumingStrat: strategy,
	}

	client, err := rabbitmq.NewClient(rabbitCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Ошибка создания клиента %s\n", err)
		os.Exit(1)
	}

	wbfPublisher := rabbitmq.NewPublisher(client, cfg.RabbitMQ.ExchangeName, "application/json")

	pub := rabbit.New(wbfPublisher)

	rep := repository.New(pgx)

	srv := service.New(rep, pub)

	h := handler.New(srv)

	router := ginext.New("debug")

	router.Use(ginext.Logger(), ginext.Recovery())

	router.POST("/notify", h.CreateNotification)
	router.GET("/notify/:id", h.GetNotification)
	router.DELETE("/notify/:id", h.DeleteNotification)

	log.Info("HTTP-сервер запускается на " + cfg.HTTP.Address)

	if err := router.Run(cfg.HTTP.Address); err != nil {
		log.Error("Ошибка запуска сервера: " + err.Error())
	}
}
