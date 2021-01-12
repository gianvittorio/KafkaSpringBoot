package com.learnkafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.entity.LibraryEventType;
import com.learnkafka.jpa.LibraryEventsRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;

@Service
@Slf4j
public class LibraryEventsService {
    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private LibraryEventsRepository libraryEventsRepository;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);

        log.info("libraryEvent : {}", libraryEvent);

        if (libraryEvent.getLibraryEventId() != null && libraryEvent.getLibraryEventId() == 000) {
            throw new RecoverableDataAccessException("Temporary network issue");
        }

        switch (libraryEvent.getLibraryEventType()) {
            case NEW:
                save(libraryEvent);

                break;
            case UPDATE:
                update(libraryEvent);

                break;
            default:
                log.info("Invalid Library Event Type");

                break;
        }
    }

    private void update(LibraryEvent libraryEvent) {
        if (libraryEvent.getLibraryEventId() == null) {
            throw new IllegalArgumentException("Library Event Id is null.");
        }

        libraryEventsRepository.findById(libraryEvent.getLibraryEventId())
                .map(record -> {
                    record.setLibraryEventType(LibraryEventType.UPDATE);
                    libraryEvent.getBook().setLibraryEvent(record);
                    record.setBook(libraryEvent.getBook());

                    libraryEventsRepository.save(record);

                    return record;
                })
                .orElseThrow(() -> new IllegalArgumentException("Library Event Id not found."));

        log.info("Library Event identified by id : {} was successfully updated.", libraryEvent.getLibraryEventId());
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);

        try {
            libraryEventsRepository.save(libraryEvent);

            log.info("Successfully persisted the Library Event {} ", libraryEvent);
        } catch (DataIntegrityViolationException e) {
            log.error("Book referred to by Id : {} exists already", libraryEvent.getBook().getBookId());

            throw new DuplicateKeyException(String.format("Book referred to by Id : %d exists already", libraryEvent.getBook().getBookId()));
        }
    }

    public void handleRecovery(ConsumerRecord<Integer, String> consumerRecord) {
        Integer key = consumerRecord.key();
        String value = consumerRecord.value();

        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, value);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable throwable) {
                handleFailure(throwable, key, value);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);
            }
        });
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info(
                "Message successfully sent for the key : {} and the value is {}, partition is {}",
                key,
                value,
                result.getRecordMetadata()
                        .partition()
        );
    }

    private void handleFailure(Throwable ex, Integer key, String value) {
        try {
            throw ex;
        } catch(Throwable throwable) {
            log.error("Error in onFailure: {}", throwable.getMessage());
        }
    }
}
