package eu.maximzin.email_notification_microservice.handler;

import com.zinoviev.core.dto.event.ProductCreatedEvent;
import eu.maximzin.email_notification_microservice.exception.NonRetryableException;
import eu.maximzin.email_notification_microservice.exception.RetryableException;
import eu.maximzin.email_notification_microservice.persistance.entity.ProcessedEventEntity;
import eu.maximzin.email_notification_microservice.persistance.repository.ProcessedEventRepository;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDateTime;

@Component
@KafkaListener(topics = "product-created-events-topic")
public class ProductCreatedEventHandler {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    private RestTemplate restTemplate;

    private ProcessedEventRepository processedEventRepository;

    public ProductCreatedEventHandler(RestTemplate restTemplate, ProcessedEventRepository processedEventRepository) {
        this.restTemplate = restTemplate;
        this.processedEventRepository = processedEventRepository;
    }

    @Transactional
    @KafkaHandler
    public void handle(@Payload ProductCreatedEvent productCreatedEvent,
                       @Header("messageId") String messageId,
                       @Header(KafkaHeaders.RECEIVED_KEY) String messageKey) {

        LOGGER.info("Received event: {}", productCreatedEvent.getTitle());

        ProcessedEventEntity processedEvent = processedEventRepository.findByMessageId(messageId);

        if (processedEvent != null) {
            LOGGER.info("Duplicate message id: {}", messageId);
            return;
        }

        try {
            String url = "http://localhost:8083/api/200";
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, null, String.class);
            if (response.getStatusCode().value() == HttpStatus.OK.value()) {
                LOGGER.info("Received response: {}", response.getBody());
            }

        } catch (ResourceAccessException e) {
            LOGGER.error(e.getMessage());
            throw new RetryableException("503", "Service Unavailable", LocalDateTime.now());
        }
        catch (HttpServerErrorException e) {
            LOGGER.error(e.getMessage());
            throw new NonRetryableException("500", "Internal Server Error", LocalDateTime.now());
        }
        catch (Exception e) {
            LOGGER.error(e.getMessage());
            throw new NonRetryableException("Unknown", "Internal Server Error", LocalDateTime.now());
        }

        try {
            processedEventRepository.save(new ProcessedEventEntity(messageId, productCreatedEvent.getProductId()));
        } catch (DataIntegrityViolationException e) {
            LOGGER.error(e.getMessage());
            throw new NonRetryableException("Unknown", "DataIntegrityViolationException", LocalDateTime.now());
        }
    }

}
