package com.learnkafka.entity;

import lombok.*;

import javax.persistence.*;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Entity
public class Book {
    @Id
    @GeneratedValue
    private Integer bookId;

    private String bookName;

    private String bookAuthor;

    @OneToOne
    @MapsId
    @JoinColumn(name = "libraryEventId")
    @ToString.Exclude
    private LibraryEvent libraryEvent;
}
