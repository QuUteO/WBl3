package main

import (
	"DelayedNotifier/internal/config"
	"fmt"
	"net/http"
	"os"
	"time"

	_ "github.com/wb-go/wbf/dbpg/pgx-driver"
	pgxdriver "github.com/wb-go/wbf/dbpg/pgx-driver"
	"github.com/wb-go/wbf/logger"
)

func main() {
	cfg, err := config.LoadConfig("/Users/mihailignatev/Desktop/WBl3/l3.1/config.yaml")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(cfg.Postgres.PostgresDSN)

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

	_, err = pgxdriver.New(
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

	mux := http.NewServeMux()

	if err := http.ListenAndServe(cfg.HTTP.Address, mux); err != nil {
		fmt.Println(err)
	}
}
