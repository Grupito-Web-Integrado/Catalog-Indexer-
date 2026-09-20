package com.example.Catalogo_Cursos_Indexer.mapper;

import com.example.Catalogo_Cursos_Indexer.document.CourseDocument;
import com.example.Catalogo_Cursos_Indexer.event.CourseCreatedEvent;
import com.example.Catalogo_Cursos_Indexer.event.course.CourseUpdateEvent;

import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

@Component
public class CourseDocumentMapper {

  /**
   * Convierte un evento de creación de curso
   * al documento de lectura de Elasticsearch.
   */
  public CourseDocument toDocument(
      CourseCreatedEvent event) {

    List<String> suggestionInputs = Stream.of(
        event.name(),
        event.code())
        .filter(Objects::nonNull)
        .map(String::trim)
        .filter(value -> !value.isBlank())
        .distinct()
        .toList();

    CourseDocument.NameSuggestion nameSuggestion = new CourseDocument.NameSuggestion(
        suggestionInputs);

    return new CourseDocument(
        event.courseId(),
        event.code(),
        nameSuggestion,
        event.status(),
        event.price(),
        event.currency(),
        event.modality(),
        null,
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

  /**
   * Convierte un evento de actualización de curso
   * al documento de lectura de Elasticsearch.
   *
   * El evento contiene el snapshot completo del curso,
   * por lo que el documento resultante representa
   * el estado completo y actualizado del curso.
   */
  public CourseDocument toDocument(
      CourseUpdateEvent event) {

    List<String> suggestionInputs = Stream.of(
        event.name(),
        event.code())
        .filter(Objects::nonNull)
        .map(String::trim)
        .filter(value -> !value.isBlank())
        .distinct()
        .toList();

    CourseDocument.NameSuggestion nameSuggestion = new CourseDocument.NameSuggestion(
        suggestionInputs);

    return new CourseDocument(
        event.courseId(),
        event.code(),
        nameSuggestion,
        event.status(),
        event.price(),
        event.currency(),
        event.modality(),
        event.imageKey(),
        event.startDate(),
        event.endDate(),
        event.startTime(),
        event.durationHours(),
        event.capacity(),
        event.availableSlots(),
        event.description(),

        // Actualmente CourseUpdateEvent no contiene createdAt.
        // Se recomienda agregarlo posteriormente para conservar
        // la fecha original de creación del curso.
        null,

        // Fecha de la última actualización del curso.
        event.updatedAt());
  }
}
