package eu.maximzin.productMicroservice;

import com.zinoviev.core.dto.event.ProductCreatedEvent;
import eu.maximzin.productMicroservice.config.KafkaConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.util.Map;


@SpringBootTest
public class IdempotentProducerIntegrationTest {

    @Autowired
    private KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;

    @MockitoBean
    KafkaAdmin kafkaAdmin;

    @Test
    void testProducerConfig_whenIdempotenceEnabled_assertsIdempotentProperties() {
        // Given
        ProducerFactory<String, ProductCreatedEvent> producerFactory = kafkaTemplate.getProducerFactory();

        // When
        Map<String, Object> config = producerFactory.getConfigurationProperties();

        // Then
        Assertions.assertEquals("true", config.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));

        Assertions.assertTrue("all".equalsIgnoreCase((String) config.get(ProducerConfig.ACKS_CONFIG)));

        if (config.containsKey(ProducerConfig.RETRIES_CONFIG)) {
            Assertions.assertTrue(Integer.valueOf(String.valueOf(config.get(ProducerConfig.RETRIES_CONFIG))) > 0);
        }

        if (config.containsKey(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)) {
            Assertions.assertTrue(Integer.valueOf(String.valueOf(config.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION))) <= 5);
        }
    }

}
