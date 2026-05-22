package eu.maximzin.email_notification_microservice;

import com.zinoviev.core.dto.event.ProductCreatedEvent;
import eu.maximzin.email_notification_microservice.handler.ProductCreatedEventHandler;
import eu.maximzin.email_notification_microservice.persistance.entity.ProcessedEventEntity;
import eu.maximzin.email_notification_microservice.persistance.repository.ProcessedEventRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.*;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.Message;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


@ActiveProfiles("test")
@EmbeddedKafka
@SpringBootTest(properties = {
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
})
public class ProductCreatedEventHandlerIntegrationTest {

    @MockitoBean
    ProcessedEventRepository processedEventRepository;

    @MockitoBean
    RestTemplate restTemplate;

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @MockitoSpyBean
    ProductCreatedEventHandler productCreatedEventHandler;

    @Test
    void testProductCreatedEventHandler_onProductCreated_handlesEvent() throws ExecutionException, InterruptedException {
        // Given
        ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent();
        productCreatedEvent.setProductId(UUID.randomUUID().toString());
        productCreatedEvent.setTitle("Sample title");
        productCreatedEvent.setPrice(new BigDecimal(300.500));
        productCreatedEvent.setQuantity(500);

        String messageId = UUID.randomUUID().toString();
        String messageKey = productCreatedEvent.getProductId();

        ProducerRecord<String, Object> record = new ProducerRecord<>(
                "product-created-events-topic",
                messageKey,
                productCreatedEvent
        );
        record.headers().add("messageId", messageId.getBytes());
        record.headers().add(KafkaHeaders.RECEIVED_KEY, messageKey.getBytes());

        ProcessedEventEntity processedEventEntity = new ProcessedEventEntity();
        when(processedEventRepository.findByMessageId(anyString())).thenReturn(processedEventEntity);
        when(processedEventRepository.save(any(ProcessedEventEntity.class))).thenReturn(null);


        String responseBody = "{\"key\":\"value\"}";
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<String> responseEntity = new ResponseEntity<>(responseBody, headers, HttpStatus.OK);

        when(restTemplate.exchange(
                any(String.class),
                any(HttpMethod.class),
                isNull(),
                eq(String.class)
        )).thenReturn(responseEntity);

        // When
        kafkaTemplate.send(record).get();

        // Then
        ArgumentCaptor<String> messageIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> messageKeyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ProductCreatedEvent> eventCaptor = ArgumentCaptor.forClass(ProductCreatedEvent.class);

        verify(productCreatedEventHandler, timeout(5000).times(1)).handle(eventCaptor.capture(),
                messageIdCaptor.capture(),
                messageKeyCaptor.capture());

        assertEquals(messageId, messageIdCaptor.getValue());
        assertEquals(messageKey, messageKeyCaptor.getValue());
        assertEquals(productCreatedEvent.getProductId(), eventCaptor.getValue().getProductId());

    }

}
