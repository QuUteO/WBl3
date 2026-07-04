package repository

import pgxdriver "github.com/wb-go/wbf/dbpg/pgx-driver"

type Repository struct {
	conn *pgxdriver.Postgres
}

func NewRepository(conn *pgxdriver.Postgres) *Repository {
	return &Repository{}
}
