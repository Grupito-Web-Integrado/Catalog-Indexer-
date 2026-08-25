package com.example.Catalogo_Cursos_Indexer.document;

import java.time.Instant;
import java.util.UUID;

/**
 * Modelo de lectura para categorías de cursos.
 */
public record CourseCategoryDocument(
    UUID courseCategoryId,
    String name,
    String status,
    String description,
    Instant createdAt,
    Instant indexedAt) {
  public static CourseCategoryDocument withIndexedAt(
      CourseCategoryDocument source,
      Instant indexedAt) {
    return new CourseCategoryDocument(
        source.courseCategoryId(),
        source.name(),
        source.status(),
        source.description(),
        source.createdAt(),
        indexedAt);
  }
}
