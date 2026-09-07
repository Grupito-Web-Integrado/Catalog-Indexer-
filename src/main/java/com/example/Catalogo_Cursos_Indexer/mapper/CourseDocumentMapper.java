package com.example.Catalogo_Cursos_Indexer.mapper;

import com.example.Catalogo_Cursos_Indexer.document.CourseDocument;
import com.example.Catalogo_Cursos_Indexer.event.CourseCreatedEvent;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Convierte el evento de dominio recibido desde Kafka al modelo
 * de lectura que se persiste en Elasticsearch.
 *
 * El campo "name" se transforma en una estructura compatible
 * con Elasticsearch Completion Suggester.
 */
@Component
public class CourseDocumentMapper {

  public CourseDocument toDocument(CourseCreatedEvent event) {

    List<String> suggestionInputs = Stream.of(
        event.name(),
        event.code())
        .filter(Objects::nonNull)
        .map(String::trim)
        .filter(value -> !value.isBlank())
        .distinct()
        .toList();

    CourseDocument.NameSuggestion nameSuggestion = new CourseDocument.NameSuggestion(suggestionInputs);

    return new CourseDocument(
        event.courseId(),
        event.code(),
        nameSuggestion,
        event.status(),
        event.price(),
        event.currency(),
        event.modality(),
        event.startDate(),
        event.endDate(),
        event.startTime(),
        event.durationHours(),
        event.capacity(),
        event.availableSlots(),
        event.description(),
        event.createdAt(),
        Instant.now());
  }
}
