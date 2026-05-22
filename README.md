## Пример работы микросервисов через Kafka

#### Предварительно нужно запустить кластер брокеров, подойдет: 
```
https://github.com/maximzin/kafka-cluster-docker-example.git
```

#### В проекте реализованы:
- Продюсер: product-microservice
- Потребитель: email-notification-microservice
- Mock-сервис для отправки callback-ов от Потребителя: mock-response-service

#### В проекте использовались следующие технологии
- Синхронная и Асихнронная (закомментировано) отправка сообщений
- Idempotence Producer
- Idempotence Consumer
- Обработка Deserialization Exception 
- Кастомные исключения для обработки Retryable и Non-Retryable ошибок
- Dead Letter Topic
- Тестирование продюсера, идемпотентности, потребителя