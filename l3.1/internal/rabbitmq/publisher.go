package rabbit

import (
	model "DelayedNotifier/internal"
	"context"
	"encoding/json"
	"fmt"

	"github.com/wb-go/wbf/rabbitmq"
)

type RabbitPublisher struct {
	publisher *rabbitmq.Publisher
}

func New(publisher *rabbitmq.Publisher) *RabbitPublisher {
	return &RabbitPublisher{publisher: publisher}
}

func (r *RabbitPublisher) Push(ctx context.Context, data *model.Notification, routingKey string) error {
	body, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("ошибка маршалинга в json: %w", err)
	}

	err = r.publisher.Publish(ctx, body, routingKey)
	if err != nil {
		return err
	}

	return nil
}
