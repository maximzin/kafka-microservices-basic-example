package eu.maximzin.productMicroservice.service;

import com.zinoviev.core.dto.event.ProductCreatedEvent;
import eu.maximzin.productMicroservice.service.dto.CreatedProductDto;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Service
public class ProductServiceImpl implements ProductService {

    private KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;
    private static final Logger LOGGER = LoggerFactory.getLogger(ProductServiceImpl.class);

    public ProductServiceImpl(KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public String createProduct(CreatedProductDto createdProductDto) throws ExecutionException, InterruptedException {
        //TODO save DB
        String productId = UUID.randomUUID().toString();

        ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent(
                productId,
                createdProductDto.title(),
                createdProductDto.price(),
                createdProductDto.quantity()
        );

//        // Асинхронный вариант
//        // начало
//        CompletableFuture<SendResult<String, ProductCreatedEvent>> future = kafkaTemplate
//                .send("product-created-events-topic", productId, productCreatedEvent);
//
//        future.whenComplete((result, exception) -> {
//            if (exception != null) {
//                LOGGER.error("Failed to send message: {}", exception.getMessage());
//            }
//            else {
//                LOGGER.info("Message send successfully: {}", result.getRecordMetadata());
//            }
//        });
//
//        LOGGER.info("Return: {}", productId);
//        // конец



        // IDEMPOTENT CONSUMER PART
        // Делаем отдельный уникальный messageId для сообщения
        ProducerRecord<String, ProductCreatedEvent> record = new ProducerRecord<>(
                "product-created-events-topic",
                productId,
                productCreatedEvent
        );
        record.headers().add("messageId", UUID.randomUUID().toString().getBytes());


        // Синхронный вариант
        // начало
        SendResult<String, ProductCreatedEvent> result = kafkaTemplate
                .send(record).get();






        LOGGER.info("Message send successfully: {}", result.getRecordMetadata());

        LOGGER.info("Return: {}", productId);
        // конец

        return null;
    }
}
