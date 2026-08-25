package com.example.Catalogo_Cursos_Indexer.document;

import java.time.Instant;
import java.util.UUID;

/**
 * Modelo de lectura para la ubicación de un curso.
 */
public record CourseLocationDocument(
    UUID courseLocationId,
    UUID courseId,
    String name,
    String address,
    String city,
    String reference,
    Integer capacity,
    Instant createdAt,
    Instant indexedAt) {
  public static CourseLocationDocument withIndexedAt(
      CourseLocationDocument source,
      Instant indexedAt) {
    return new CourseLocationDocument(
        source.courseLocationId(),
        source.courseId(),
        source.name(),
        source.address(),
        source.city(),
        source.reference(),
        source.capacity(),
        source.createdAt(),
        indexedAt);
  }
}
