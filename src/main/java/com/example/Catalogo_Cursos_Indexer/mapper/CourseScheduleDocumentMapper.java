package com.example.Catalogo_Cursos_Indexer.mapper;

import com.example.Catalogo_Cursos_Indexer.document.CourseScheduleDocument;
import com.example.Catalogo_Cursos_Indexer.event.CourseScheduleCreatedEvent;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Convierte el evento de horario recibido desde Kafka
 * al documento de lectura para Elasticsearch.
 */
@Component
public class CourseScheduleDocumentMapper {

  public CourseScheduleDocument toDocument(
      CourseScheduleCreatedEvent event) {
    return new CourseScheduleDocument(
        event.courseScheduleId(),
        event.courseId(),
        event.dayOfWeek(),
        event.startTime(),
        event.endTime(),
        event.room(),
        event.createdAt(),
        Instant.now());
  }
}
