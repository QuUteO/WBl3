package main

import (
	"DelayedNotifier/internal/config"
	"fmt"
	"net/http"

	_ "github.com/wb-go/wbf/dbpg/pgx-driver"
)

func main() {
	cfg, err := config.LoadConfig("/Users/mihailignatev/Desktop/WBl3/l3.1/config.yaml")
	if err != nil {
		fmt.Println(err)
	}

	mux := http.NewServeMux()

	if err := http.ListenAndServe(cfg.HTTP.Address, mux); err != nil {
		fmt.Println(err)
	}
}
