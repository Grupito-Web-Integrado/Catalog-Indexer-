package com.example.Catalogo_Cursos_Indexer.document;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

/**
 * Modelo de lectura denormalizado para el índice "courses" en Elasticsearch.
 * Optimizado para búsqueda y consulta, no para escritura transaccional.
 */
public record CourseDocument(
    UUID courseId,
    String code,
    String name,
    String status,
    BigDecimal price,
    String currency,
    String modality,
    LocalDate startDate,
    LocalDate endDate,
    LocalTime startTime,
    Integer durationHours,
    Integer capacity,
    Integer availableSlots,
    String description,
    Instant createdAt,
    Instant indexedAt) {
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
