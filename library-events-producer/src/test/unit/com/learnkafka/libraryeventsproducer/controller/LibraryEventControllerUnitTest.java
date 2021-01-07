package com.learnkafka.libraryeventsproducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.libraryeventsproducer.domain.Book;
import com.learnkafka.libraryeventsproducer.domain.LibraryEvent;
import com.learnkafka.libraryeventsproducer.producer.LibraryEventProducer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.match.MockRestRequestMatchers;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.RequestBuilder;
import org.springframework.test.web.servlet.ResultMatcher;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;

@ExtendWith(MockitoExtension.class)
@WebMvcTest(LibraryEventsController.class)
@AutoConfigureMockMvc
public class LibraryEventControllerUnitTest {
    @Autowired
    MockMvc mvc;

    @MockBean
    LibraryEventProducer libraryEventProducer;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("Must post library event.")
    public void postLibraryEventTest() throws Exception {
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

        when(libraryEventProducer.sendLibraryEvent(isA(LibraryEvent.class)))
                .thenReturn(null);

        // When
        RequestBuilder request = post("/api/v1/libraryevent")
                .contentType(MediaType.APPLICATION_JSON_VALUE)
                .accept(MediaType.APPLICATION_JSON_VALUE)
                .content(objectMapper.writeValueAsString(libraryEvent));

        mvc.perform(request)
                .andExpect(status().isCreated());

        verify(libraryEventProducer)
                .sendLibraryEvent(isA(LibraryEvent.class));
    }

    @Test
    @DisplayName("Must throw whenever posting invalid library event.")
    public void postLibraryEvent4XXTest() throws Exception {
        // Given
        Book book = Book.builder()
                .bookId(null)
                .bookAuthor(null)
                .bookName(null)
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        when(libraryEventProducer.sendLibraryEvent(isA(LibraryEvent.class)))
                .thenReturn(null);

        // When
        RequestBuilder request = post("/api/v1/libraryevent")
                .contentType(MediaType.APPLICATION_JSON_VALUE)
                .accept(MediaType.APPLICATION_JSON_VALUE)
                .content(objectMapper.writeValueAsString(libraryEvent));

        // Then
        String expectedErrorMessage = "book.bookAuthor - must not be blank, book.bookId - must not be null, book.bookName - must not be blank";
        mvc.perform(request)
                .andExpect(status().is4xxClientError())
                .andExpect(MockMvcResultMatchers.content().string(expectedErrorMessage));
    }

    @Test
    @DisplayName("Must update library event.")
    public void putLibraryEventTest() throws Exception {
        // Given
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Dilip")
                .bookName("Kafka Using SpringBoot")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(1)
                .book(book)
                .build();

        when(libraryEventProducer.sendLibraryEvent(isA(LibraryEvent.class)))
                .thenReturn(null);

        // When
        MockHttpServletRequestBuilder request = put("/api/v1/libraryevent")
                .contentType(MediaType.APPLICATION_JSON_VALUE)
                .accept(MediaType.APPLICATION_JSON_VALUE)
                .content(objectMapper.writeValueAsString(libraryEvent));

        // Then
        mvc.perform(request)
                .andExpect(status().isOk());

        verify(libraryEventProducer)
                .sendLibraryEvent(isA(LibraryEvent.class));
    }

    @Test
    @DisplayName("Must return 400 whenever provided libraryEventId is null.")
    public void putLibraryEventFailureTest() throws Exception {
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

        when(libraryEventProducer.sendLibraryEvent(isA(LibraryEvent.class)))
                .thenReturn(null);

        // When
        MockHttpServletRequestBuilder request = put("/api/v1/libraryevent")
                .contentType(MediaType.APPLICATION_JSON_VALUE)
                .accept(MediaType.APPLICATION_JSON_VALUE)
                .content(objectMapper.writeValueAsString(libraryEvent));

        // Then
        mvc.perform(request)
                .andExpect(status().is4xxClientError())
        .andExpect(content().string("Please, provide libraryEventId."));

        verify(libraryEventProducer, never())
                .sendLibraryEvent(isA(LibraryEvent.class));
    }
}
