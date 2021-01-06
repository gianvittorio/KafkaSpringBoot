package com.learnkafka.libraryeventsproducer.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class LibraryEvent {
    private Integer libraryEventId;
    private LibraryEventType libraryEventType;
    private Book book;
}
