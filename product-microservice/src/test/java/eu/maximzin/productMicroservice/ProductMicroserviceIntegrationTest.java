package eu.maximzin.productMicroservice;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.zinoviev.core.dto.event.ProductCreatedEvent;
import eu.maximzin.productMicroservice.service.ProductService;
import eu.maximzin.productMicroservice.service.dto.CreatedProductDto;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import scala.collection.immutable.Stream;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@DirtiesContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 3, count = 1, controlledShutdown = true)
@SpringBootTest(properties = "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}")
public class ProductMicroserviceIntegrationTest {

    @Autowired
    private ProductService productService;

    @Autowired
    private Environment environment;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker; // Тут подчеркивается красным но ОК

    private KafkaMessageListenerContainer<String, ProductCreatedEvent> container;

    private BlockingQueue<ConsumerRecord<String, ProductCreatedEvent>> records;

    @BeforeAll
    void setUp() {
        DefaultKafkaConsumerFactory<String, Object> consumerFactory = new DefaultKafkaConsumerFactory<>(getConsumerProperties());

        ContainerProperties containerProperties = new ContainerProperties(environment.getProperty("product-created-events-topic-name"));

        container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
        records = new LinkedBlockingDeque<>();
        container.setupMessageListener((MessageListener<String, ProductCreatedEvent>) records::add);

        container.start();

        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
    }

    private Map<String, Object> getConsumerProperties() {

        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class,
                ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JacksonJsonDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, environment.getProperty("spring.kafka.consumer.group-id"),
                JacksonJsonDeserializer.TRUSTED_PACKAGES, environment.getProperty("spring.kafka.consumer.properties.spring.json.trusted.packages")
        );

    }

    @AfterAll
    void tearDown() {
        container.stop();
    }

    @Test
    void testCreateProduct_whenGivenValidProductDetails_successfullySendsKafkaMessage() throws ExecutionException, InterruptedException {
        // Given
        String title = "Sample title";
        BigDecimal price = new BigDecimal(300.500);
        Integer quantity = 500;

        CreatedProductDto createdProductDto = new CreatedProductDto(title, price, quantity);

        // When
        productService.createProduct(createdProductDto);

        // Then
        ConsumerRecord<String, ProductCreatedEvent> incomeMessage = records.poll(3000, TimeUnit.MILLISECONDS);

        assertNotNull(incomeMessage);
        assertNotNull(incomeMessage.key());
        ProductCreatedEvent productCreatedEvent = incomeMessage.value();
        assertEquals(productCreatedEvent.getTitle(), title);
        assertEquals(productCreatedEvent.getPrice(), price);
        assertEquals(productCreatedEvent.getQuantity(), quantity);

    }



}
