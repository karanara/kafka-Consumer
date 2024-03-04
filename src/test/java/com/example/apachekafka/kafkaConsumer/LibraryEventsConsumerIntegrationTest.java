package com.example.apachekafka.kafkaConsumer;

import com.example.apachekafka.kafkaConsumer.consumer.LibraryEventConsumer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.apachekafka.kafkaConsumer.service.LibraryEventsService;
import com.example.apachekafka.kafkaConsumer.entity.LibraryEventType;
import com.example.apachekafka.kafkaConsumer.entity.Book;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import com.example.apachekafka.kafkaConsumer.entity.LibraryEvent;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.example.apachekafka.kafkaConsumer.jpa.FailureRecordRepository;
import com.example.apachekafka.kafkaConsumer.jpa.LibraryEventsRepository;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;


@SpringBootTest
@EmbeddedKafka(topics = {"library-events","library-events.RETRY","library-events.DLT"} , partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"
        , "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}","retryListener.startup=false"}
        )
public class LibraryEventsConsumerIntegrationTest {


    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @SpyBean
    LibraryEventConsumer libraryEventsConsumerSpy;

    @SpyBean
    LibraryEventsService libraryEventsServiceSpy;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;
    
    @Autowired
    FailureRecordRepository failureRecordRepository;
    
    @Value("${topics.retry:library-events.RETRY}")
	private String retryTopic;
	
	@Value("${topics.dlt:library-events.DLT}")
	private String deadLetterTopic;
	
    private Consumer<Integer, String> consumer;


    @Autowired
    ObjectMapper objectMapper;


    @BeforeEach
    void setUp() {
    	//for one particular consumer 
    	var container = endpointRegistry.getListenerContainers()
                .stream().filter(messageListenerContainer ->
                        Objects.equals(messageListenerContainer.getGroupId(), "library-events-listener-group"))
                .collect(Collectors.toList()).get(0);
    	ContainerTestUtils.waitForAssignment(container,embeddedKafkaBroker.getPartitionsPerTopic());
        //for (MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()) {
          //  ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        //}
    }

    @AfterEach
    void tearDown() {

        libraryEventsRepository.deleteAll();

    }

    @Test
    void publishNewLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        //given
        String json = " {\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(json).get();

        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        //then
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> libraryEventList = (List<LibraryEvent>) libraryEventsRepository.findAll();
        assert libraryEventList.size() == 1;
        libraryEventList.forEach(libraryEvent -> {
            assert libraryEvent.getLibraryEventId() != null;
            assertEquals(456, libraryEvent.getBook().getBookId());
        });

    }


    @Test
    void publishUpdateLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        //given
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        //publish the update LibraryEvent

        Book updatedBook = Book.builder().
                bookId(456).bookName("Kafka Using Spring Boot Ramya").bookAuthor("Dilip").build();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);
        String updatedJson = objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson).get();

        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        //then
        //verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        //verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
        LibraryEvent persistedLibraryEvent = libraryEventsRepository.findById(libraryEvent.getLibraryEventId()).get();
        assertEquals("Kafka Using Spring Boot Ramya", persistedLibraryEvent.getBook().getBookName());
    }


    @Test
    void publishModifyLibraryEvent_Null_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
        //given
        Integer libraryEventId = null;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);


        verify(libraryEventsConsumerSpy, times(0)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(0)).processLibraryEvent(isA(ConsumerRecord.class));
        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group2", "true", embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, deadLetterTopic);

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, deadLetterTopic);

        System.out.println("consumer Record in deadletter topic : " + consumerRecord.value());
        
        assertEquals(json, consumerRecord.value());
        consumerRecord.headers()
                .forEach(header -> {
                    System.out.println("Header Key : " + header.key() + ", Header Value : " + new String(header.value()));
                });
    }
    @Test
    void publishModifyLibraryEvent_999_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
        //given
        Integer libraryEventId = 999;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);


        verify(libraryEventsConsumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(3)).processLibraryEvent(isA(ConsumerRecord.class));

    }
    
    @Test
    void publishModifyLibraryEvent_999_retryTopic_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
        //given
        Integer libraryEventId = 999;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);


        verify(libraryEventsConsumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(3)).processLibraryEvent(isA(ConsumerRecord.class));

        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, retryTopic);

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, retryTopic);

        System.out.println("consumer Record in retry topic : " + consumerRecord.value());

        assertEquals(json, consumerRecord.value());
        consumerRecord.headers()
        .forEach(header -> {
            System.out.println("Header Key : " + header.key() + ", Header Value : " + new String(header.value()));
        });
    }
    @Test
    void publishModifyLibraryEvent_999_LibraryEventId_failureRecord() throws JsonProcessingException, InterruptedException, ExecutionException {
        //given
        Integer libraryEventId = 999;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);


        verify(libraryEventsConsumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(3)).processLibraryEvent(isA(ConsumerRecord.class));


        var failureCount = failureRecordRepository.count();
        assertEquals(1, failureCount);
        failureRecordRepository.findAll().forEach(failureRecord -> {
            System.out.println("failureRecord : " + failureRecord);
        });

    }

}
