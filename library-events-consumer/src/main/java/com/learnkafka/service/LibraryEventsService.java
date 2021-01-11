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
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;

@Service
@Slf4j
public class LibraryEventsService {
    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private LibraryEventsRepository libraryEventsRepository;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);

        log.info("libraryEvent : {}", libraryEvent);

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
}
