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
	"github.com/wb-go/wbf/redis"
	"github.com/wb-go/wbf/retry"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "config.yaml"
	}

	cfg, err := config.LoadConfig(configPath)
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

	// 1. Инициализация Базы Данных (Postgres)
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

	// 2. Инициализация и подключение к Redis
	redisOpts := redis.Options{
		Address:   cfg.Redis.Address,
		Password:  cfg.Redis.Password,
		MaxMemory: cfg.Redis.MaxMemory,
		Policy:    cfg.Redis.Policy,
	}

	redisClient, err := redis.Connect(redisOpts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Ошибка подключения к Redis: %v\n", err)
		os.Exit(1)
	}
	log.Info("Redis успешно подключен и настроен")

	// Закрываем клиент Redis при выходе из приложения
	defer func() {
		if err := redisClient.Close(); err != nil {
			log.Error(fmt.Sprintf("Ошибка при закрытии Redis: %v", err))
		} else {
			log.Info("Соединение с Redis успешно закрыто.")
		}
	}()

	// Стратегия ретраев для RabbitMQ
	strategy := retry.Strategy{
		Attempts: 5,
		Delay:    3 * time.Second,
		Backoff:  2,
	}

	// Отдельная быстрая стратегия ретраев для Redis кэша
	redisStrategy := retry.Strategy{
		Attempts: 3,
		Delay:    100 * time.Millisecond,
		Backoff:  1,
	}

	// 3. Настройка RabbitMQ
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

	// 4. Сборка слоев (Dependency Injection) с добавлением Redis
	rep := repository.New(pgx, redisClient, redisStrategy)
	srv := service.New(rep, pubAdapter, cfg.Telegram.Token, &cfg.SMTP)
	h := handler.New(srv)

	// 5. Логика обработки сообщений (Воркер)
	messageProcessor := func(ctx context.Context, d amqp091.Delivery) error {
		log.Info(fmt.Sprintf("[RABBITMQ] Получены сырые байты из очереди! Размер: %d байт. RoutingKey: %s", len(d.Body), d.RoutingKey))

		var notification model.Notification
		err := json.Unmarshal(d.Body, &notification)
		if err != nil {
			log.Error(fmt.Sprintf("[RABBITMQ ERROR] Не удалось распарсить JSON сообщения: %v. Сырые данные: %s", err, string(d.Body)))
			return err
		}

		log.Info(fmt.Sprintf("[WORKER] Начинаем обработку уведомления ID: %s, Канал: %s, Получатель: %s", notification.ID, notification.Channel, notification.Recipient))

		err = srv.ProcessNotification(ctx, &notification)
		if err != nil {
			if errors.Is(err, service.ErrTooEarly) {
				log.Info(fmt.Sprintf("[WORKER] Уведомление %s пришло слишком рано, возвращено в расписание", notification.ID))
				return nil
			}
			log.Error(fmt.Sprintf("[WORKER ERROR] Ошибка внутри SendToExternalAPI для %s: %v", notification.ID, err))
			return nil
		}

		log.Info(fmt.Sprintf("[SUCCESS] Уведомление %s успешно обработано, отправлено на внешнее API и обновлено в БД!", notification.ID))
		return nil
	}

	queueArgs := amqp091.Table{}

	consumerCfg := rabbitmq.ConsumerConfig{
		Queue: "notification-queue",
		Args:  queueArgs,
	}

	consumer := rabbitmq.NewConsumer(client, consumerCfg, messageProcessor)

	// Запуск потребителя RabbitMQ
	go func() {
		log.Info("Фоновый воркер (Consumer) успешно запущен и ждет сообщения...")
		if err := consumer.Start(ctx); err != nil {
			log.Error(fmt.Sprintf("Ошибка при потреблении сообщений: %v", err))
			fmt.Printf("\nКРИТИЧЕСКАЯ ОШИБКА ЗАПУСКА ВОРКЕРА: %v\n\n", err)
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

	// 6. Запуск HTTP Сервера
	router := ginext.New("debug")
	router.Use(ginext.Logger(), ginext.Recovery())

	router.POST("/notify", h.CreateNotification)
	router.GET("/notify", h.ListNotifications)
	router.GET("/notify/:id", h.GetNotification)
	router.DELETE("/notify/:id", h.DeleteNotification)
	router.GET("/", func(c *ginext.Context) {
		c.File("web/index.html")
	})

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
