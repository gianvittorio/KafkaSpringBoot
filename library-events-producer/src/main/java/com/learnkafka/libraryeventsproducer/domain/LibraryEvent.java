package com.learnkafka.libraryeventsproducer.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotNull;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class LibraryEvent {
    private Integer libraryEventId;

    private LibraryEventType libraryEventType;

    @NotNull
    private Book book;
}
