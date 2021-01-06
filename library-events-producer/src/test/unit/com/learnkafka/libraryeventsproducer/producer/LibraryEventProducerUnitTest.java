package com.learnkafka.libraryeventsproducer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.libraryeventsproducer.domain.Book;
import com.learnkafka.libraryeventsproducer.domain.LibraryEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class LibraryEventProducerUnitTest {
    @Mock
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Spy
    ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    LibraryEventProducer libraryEventProducer;

    @Test
    @DisplayName("Must assert onFailure.")
    public void sendLibraryEventFailureTest() throws JsonProcessingException, ExecutionException, InterruptedException {
        // Given
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Dilip")
                .bookName("Kafka Using SpringBoot")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        SettableListenableFuture future = new SettableListenableFuture();
        future.setException(new RuntimeException("Exception calling Kafka"));

        when(kafkaTemplate.send(isA(ProducerRecord.class)))
                .thenReturn(future);

        // When
        org.junit.jupiter.api.Assertions.assertThrows(
                Exception.class,
                () -> libraryEventProducer.sendLibraryEvent(libraryEvent)
                        .get()
        );
    }

    @Test
    @DisplayName("Must assert onSuccess.")
    public void sendLibraryEventSuccessTest() throws JsonProcessingException, ExecutionException, InterruptedException {
        // Given
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Dilip")
                .bookName("Kafka Using SpringBoot")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        SettableListenableFuture future = new SettableListenableFuture();

        String record = objectMapper.writeValueAsString(libraryEvent);
        ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>("library-events", libraryEvent.getLibraryEventId(), record);
        RecordMetadata recordMetaData = new RecordMetadata(
                new TopicPartition("library-events", 1),
                1,
                1,
                342,
                System.currentTimeMillis(),
                1,
                2
        );

        SendResult<Integer, String> sendResult = new SendResult<>(producerRecord, recordMetaData);
        future.set(sendResult);

        when(kafkaTemplate.send(isA(ProducerRecord.class)))
                .thenReturn(future);

        // When
        ListenableFuture<SendResult<Integer, String>> listenableFuture = libraryEventProducer.sendLibraryEvent(libraryEvent);

        // Then
        SendResult<Integer, String> sendResult1 = listenableFuture.get();
        assertEquals(1, sendResult1.getRecordMetadata().partition());
    }
}
