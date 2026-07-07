package handler

import (
	model "DelayedNotifier/internal"
	"context"
	"net/http"

	// Подставь сюда правильный путь к твоему пакету ginext в проекте

	"github.com/google/uuid"
	"github.com/wb-go/wbf/ginext"
)

type NotificationService interface {
	CreateNotification(ctx context.Context, req *model.CreateNotification) (*model.Notification, error)
	GetNotification(ctx context.Context, id uuid.UUID) (*model.CreateNotification, error)
	DeleteNotification(ctx context.Context, id uuid.UUID) error
}

type Handler struct {
	service NotificationService
}

func New(service NotificationService) *Handler {
	return &Handler{
		service: service,
	}
}

// CreateNotification POST /notify
func (h *Handler) CreateNotification(c *ginext.Context) {
	var req model.CreateNotification

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ginext.H{"error": err.Error()})
		return
	}

	notification, err := h.service.CreateNotification(c.Request.Context(), &req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ginext.H{"error": err.Error()})
		return
	}

	// Быстрая отправка JSON с правильным статус-кодом
	c.JSON(http.StatusCreated, notification)
}

// GetNotification GET /notify/:id
func (h *Handler) GetNotification(c *ginext.Context) {
	id := c.Param("id")

	ID, err := uuid.Parse(id)
	if err != nil {
		c.JSON(http.StatusBadRequest, ginext.H{"error": err.Error()})
		return
	}

	notification, err := h.service.GetNotification(c.Request.Context(), ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ginext.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, notification)
}

// DeleteNotification DELETE /notify/:id
func (h *Handler) DeleteNotification(c *ginext.Context) {
	id := c.Param("id")

	ID, err := uuid.Parse(id)
	if err != nil {
		c.JSON(http.StatusBadRequest, ginext.H{"error": err.Error()})
		return
	}

	err = h.service.DeleteNotification(c.Request.Context(), ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ginext.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, ginext.H{"status": "deleted"})
}
