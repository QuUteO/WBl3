package handler

import (
	model "DelayedNotifier/internal"
	"context"
	"net/http"

	"github.com/google/uuid"
)

type NotificationService interface {
	CreateNotification(ctx context.Context, notification *model.CreateNotification) error
	GetNotification(ctx context.Context, id uuid.UUID) (*model.CreateNotification, error)
	DeleteNotification(ctx context.Context, id uuid.UUID) error
}

type Handler struct {
	service NotificationService
}

func New(service NotificationService) *Handler {
	return &Handler{service: service}
}

func CreateNotification(w http.ResponseWriter, r *http.Request) {

}
