package main

import (
	model "DelayedNotifier/internal"
	"DelayedNotifier/internal/config"
	"DelayedNotifier/internal/handler"
	pub "DelayedNotifier/internal/rabbitmq"
	"DelayedNotifier/internal/repository"
	"DelayedNotifier/internal/service"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/wb-go/wbf/dbpg/pgx-driver"
	"github.com/wb-go/wbf/ginext"
	"github.com/wb-go/wbf/logger"
	"github.com/wb-go/wbf/rabbitmq"
	"github.com/wb-go/wbf/retry"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

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

	pubAdapter := pub.New(wbfPublisher)

	rep := repository.New(pgx)
	srv := service.New(rep, pubAdapter, cfg.Telegram.Token, &cfg.SMTP)
	h := handler.New(srv)

	messageProcessor := func(ctx context.Context, d amqp091.Delivery) error {
		var notification model.Notification
		err := json.Unmarshal(d.Body, &notification)
		if err != nil {
			log.Error("Ошибка анмаршалинга сообщения")
			return err
		}

		err = srv.ProcessNotification(ctx, &notification)
		if err != nil {
			if errors.Is(err, service.ErrTooEarly) {
				log.Info(fmt.Sprintf("Уведомление %s пришло слишком рано, возвращено в расписание", notification.ID))
				return nil
			}
			log.Error(fmt.Sprintf("Воркер не смог отправить уведомление %s: %v", notification.ID, err))
			return nil
		}

		log.Info(fmt.Sprintf("Уведомление %s успешно обработано и отправлено", notification.ID))
		return nil
	}

	queueArgs := amqp091.Table{
		"x-dead-letter-exchange":    "dlx",
		"x-dead-letter-routing-key": "test.queue.dlq",
	}

	consumerCfg := rabbitmq.ConsumerConfig{
		Queue: "notification-queue",
		Args:  queueArgs,
	}

	consumer := rabbitmq.NewConsumer(client, consumerCfg, messageProcessor)

	go func() {
		log.Info("Фоновый воркер (Consumer) успешно запущен и ждет сообщения...")
		if err := consumer.Start(ctx); err != nil {
			log.Error(fmt.Sprintf("Ошибка при потреблении сообщений: %v", err))
		}
	}()

	// Фоновый планировщик отложенных уведомлений (Тикер раз в 10 секунд)
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		log.Info("Фоновый планировщик отложенных уведомлений успешно запущен...")

		for {
			select {
			case <-ctx.Done():
				log.Info("Фоновый планировщик останавливается...")
				return
			case <-ticker.C:
				scanCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				if err := srv.CheckAndPublishDelayed(scanCtx); err != nil {
					log.Error(fmt.Sprintf("Ошибка при сканировании отложенных уведомлений: %v", err))
				}
				cancel()
			}
		}
	}()

	router := ginext.New("debug")
	router.Use(ginext.Logger(), ginext.Recovery())

	router.POST("/notify", h.CreateNotification)
	router.GET("/notify/:id", h.GetNotification)
	router.DELETE("/notify/:id", h.DeleteNotification)

	go func() {
		log.Info("HTTP-сервер запускается на " + cfg.HTTP.Address)
		if err := router.Run(cfg.HTTP.Address); err != nil {
			log.Error(fmt.Sprintf("Ошибка запуска сервера: %v", err))
		}
	}()

	<-ctx.Done()

	log.Info("Получен сигнал завершения. Мягко останавливаем приложение (Graceful Shutdown)...")
	time.Sleep(2 * time.Second)
	log.Info("Приложение полностью остановлено.")
}
