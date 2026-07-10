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

	fmt.Printf("[PUBLISHER] Пытаемся отправить сообщение! ID: %s, RoutingKey: %s\n", data.ID, routingKey)

	err = r.publisher.Publish(ctx, body, routingKey)
	if err != nil {
		fmt.Printf("[PUBLISHER ERROR] Ошибка при вызове r.publisher.Publish: %v\n", err)
		return err
	}

	fmt.Printf("[PUBLISHER SUCCESS] Сообщение успешно ушло в RabbitMQ! ID: %s\n", data.ID)
	return nil
}
