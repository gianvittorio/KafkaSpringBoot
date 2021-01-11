package com.learnkafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.entity.LibraryEventType;
import com.learnkafka.jpa.LibraryEventsRepository;
import com.learnkafka.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

@SpringBootTest
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(
        properties = {
                "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
                "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"
        }
)
public class LibraryEventsConsumerIntegrationTest {
    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @SpyBean
    LibraryEventsConsumer libraryEventsConsumerSpy;

    @SpyBean
    LibraryEventsService libraryEventsServiceSpy;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;

    @Autowired
    ObjectMapper objectMapper;

    @BeforeEach
    public void setUp() {
        kafkaListenerEndpointRegistry.getAllListenerContainers()
                .forEach(
                        messageListenerContainer -> ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic()
                        )
                );
    }

    @AfterEach
    public void tearDown() throws InterruptedException {
        libraryEventsRepository.deleteAll();
        new CountDownLatch(1).await(2, TimeUnit.SECONDS);
    }

    @Test
    @DisplayName("Must publish new Library Event.")
    public void publishNewLibraryEventTest() throws ExecutionException, InterruptedException, JsonProcessingException {
        // Given
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":" +
                "{\"bookId\":456,\"bookName\":\"Kafka Using SpringBoot\",\"bookAuthor\":\"Dilip\"}}";

        kafkaTemplate.sendDefault(json)
                .get();

        // When
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(3, TimeUnit.SECONDS);

        // Then
        verify(libraryEventsConsumerSpy)
                .onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy)
                .processLibraryEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> libraryEventList = (List<LibraryEvent>) libraryEventsRepository.findAll();
        assertEquals(1, libraryEventList.size());
        libraryEventList.forEach(
                libraryEvent -> {
                    assertNotNull(libraryEvent.getLibraryEventId());
                    assertEquals(456, libraryEvent.getBook().getBookId());
                }
        );
    }

    @Test
    @DisplayName("Must update existing Library Event.")
    public void publishUpdateLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        // Given
        String json = "{\"libraryEventId\":1,\"libraryEventType\":\"UPDATE\",\"book\":" +
                "{\"bookId\":123,\"bookName\":\"Kafka Using SpringBoot\",\"bookAuthor\":\"Dilip\"}}";

        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);

        libraryEventsRepository.save(libraryEvent);

        Book updatedBook = Book.builder().
                bookId(456)
                .bookName("Kafka Spring Cloud")
                .bookAuthor("Dilip")
                .build();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);
        String updatedJson = objectMapper.writeValueAsString(libraryEvent);

        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson).get();

        // When
        new CountDownLatch(1)
                .await(3, TimeUnit.SECONDS);

        // Then
        verify(libraryEventsConsumerSpy)
                .onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy)
                .processLibraryEvent(isA(ConsumerRecord.class));

        Optional<LibraryEvent> persistedLibraryEvent = libraryEventsRepository.findById(libraryEvent.getLibraryEventId());
        assertTrue(persistedLibraryEvent.isPresent());
        assertEquals(updatedBook.getBookName(), persistedLibraryEvent.get().getBook().getBookName());
    }

    @Test
    @DisplayName("Must not find any LibraryEvent referred to by non existing id.")
    public void publishUpdateNonExistingLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        // Given
        int randomId = 123;
        String json = String.format("{\"libraryEventId\": %d,\"libraryEventType\":\"UPDATE\",\"book\":" +
                "{\"bookId\":123,\"bookName\":\"Kafka Using SpringBoot\",\"bookAuthor\":\"Dilip\"}}", randomId);

        kafkaTemplate.sendDefault(json)
                .get();

        // When
        new CountDownLatch(1)
                .await(3, TimeUnit.SECONDS);

        // Then
        verify(libraryEventsConsumerSpy, atLeastOnce())
                .onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, atLeastOnce())
                .processLibraryEvent(isA(ConsumerRecord.class));

        assertTrue(libraryEventsRepository.findById(randomId).isEmpty());
    }

    @Test
    @DisplayName("Must throw IllegalArgumentException whenever modifying Library Event with invalid libraryEventId.")
    public void publishUpdateInvalidLibraryEvent() throws JsonProcessingException, InterruptedException, ExecutionException {
        // Given
        String json = "{\"libraryEventId\": null,\"libraryEventType\":\"UPDATE\",\"book\":" +
                "{\"bookId\":123,\"bookName\":\"Kafka Using SpringBoot\",\"bookAuthor\":\"Dilip\"}}";

        kafkaTemplate.sendDefault(json)
                .get();

        // When
        new CountDownLatch(1)
                .await(3, TimeUnit.SECONDS);

        // Then
        verify(libraryEventsConsumerSpy, atLeast(1))
                .onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, atLeast(1))
                .processLibraryEvent(isA(ConsumerRecord.class));
    }
}
