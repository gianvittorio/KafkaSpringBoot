package com.learnkafka.libraryeventsproducer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.libraryeventsproducer.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Component
@Slf4j
public class LibraryEventProducer {
    private static final String TOPIC = "library-events";

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    public SendResult<Integer, String> sendLibraryEventSynchronously(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        SendResult<Integer, String> sendResult = null;
        try {
            sendResult = kafkaTemplate.sendDefault(key, value)
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("InterruptedException/ExecutionException sending message and the exception is {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Exception sending message and the exception is {}", e.getMessage());
            throw e;
        }

        return sendResult;
    }

    public SendResult<Integer, String> sendLibraryEventSynchronouslyApproach2(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value, TOPIC);
        SendResult<Integer, String> sendResult = null;
        try {
            sendResult = kafkaTemplate.send(producerRecord)
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("InterruptedException/ExecutionException sending message and the exception is {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Exception sending message and the exception is {}", e.getMessage());
            throw e;
        }

        return sendResult;
    }

    private static ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {
        List<Header> recordHeaders = Collections.singletonList(new RecordHeader("event-source", "scanner".getBytes()));

        return new ProducerRecord<>(topic, null, key, value, recordHeaders);
    }

    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value, TOPIC);

        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(producerRecord);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable throwable) {
                handleFailure(key, value, throwable);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> integerStringSendResult) {
                handleSuccess(key, value, integerStringSendResult);
            }
        });
    }

    private void handleFailure(Integer key, String value, Throwable throwable) {
        log.error("Error sending message and the exception is {}", throwable.getMessage());

        try {
            throw throwable;
        } catch (Throwable t) {
            log.error("Error in OnFailure: {}", t.getMessage());
        }
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> integerStringSendResult) {
        log.info(
                "Message successfully sent for the key : {} and the value is {}, partition is {}",
                key,
                value,
                integerStringSendResult.getRecordMetadata()
                        .partition()
        );
    }
}
