package com.learnkafka.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class LibraryEvent {
    private Integer libraryEventId;
    private Book book;
    private LibraryEventType libraryEventType;


    public void setLibraryEventId(Integer libraryEventId) {
        this.libraryEventId = libraryEventId;
    }
}
