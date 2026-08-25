package com.example.Catalogo_Cursos_Indexer.document;

import java.time.Instant;
import java.time.LocalTime;
import java.util.UUID;

/**
 * Modelo de lectura para el horario de un curso.
 */
public record CourseScheduleDocument(
    UUID courseScheduleId,
    UUID courseId,
    String dayOfWeek,
    LocalTime startTime,
    LocalTime endTime,
    String room,
    Instant createdAt,
    Instant indexedAt) {
  public static CourseScheduleDocument withIndexedAt(
      CourseScheduleDocument source,
      Instant indexedAt) {
    return new CourseScheduleDocument(
        source.courseScheduleId(),
        source.courseId(),
        source.dayOfWeek(),
        source.startTime(),
        source.endTime(),
        source.room(),
        source.createdAt(),
        indexedAt);
  }
}
