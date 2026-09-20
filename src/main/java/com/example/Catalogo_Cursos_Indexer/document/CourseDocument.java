package com.example.Catalogo_Cursos_Indexer.document;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;

/**
 * Modelo de lectura denormalizado para el índice "courses"
 * en Elasticsearch.
 *
 * El campo "name" se indexa como completion suggester.
 * Por ello, name contiene las entradas utilizadas por
 * Elasticsearch para autocompletado.
 */
public record CourseDocument(
    UUID courseId,
    String code,
    NameSuggestion name,
    String status,
    BigDecimal price,
    String currency,
    String modality,
    String imageKey,
    LocalDate startDate,
    LocalDate endDate,
    LocalTime startTime,
    Integer durationHours,
    Integer capacity,
    Integer availableSlots,
    String description,
    Instant createdAt,
    Instant indexedAt) {

  /**
   * Representa el campo "name" de tipo "completion"
   * en Elasticsearch.
   *
   * Ejemplo JSON:
   *
   * "name": {
   * "input": [
   * "Java Programming",
   * "JAVA-001"
   * ]
   * }
   */
  public record NameSuggestion(
      List<String> input) {
  }

  /**
   * Crea una copia del documento actualizando únicamente
   * el timestamp de indexación.
   */
  public static CourseDocument withIndexedAt(
      CourseDocument source,
      Instant indexedAt) {

    return new CourseDocument(
        source.courseId(),
        source.code(),
        source.name(),
        source.status(),
        source.price(),
        source.currency(),
        source.modality(),
        source.imageKey(),
        source.startDate(),
        source.endDate(),
        source.startTime(),
        source.durationHours(),
        source.capacity(),
        source.availableSlots(),
        source.description(),
        source.createdAt(),
        indexedAt);
  }
}
