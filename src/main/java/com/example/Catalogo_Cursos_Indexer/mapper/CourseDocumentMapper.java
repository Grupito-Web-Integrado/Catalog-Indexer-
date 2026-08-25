package com.example.Catalogo_Cursos_Indexer.mapper;

import com.example.Catalogo_Cursos_Indexer.document.CourseDocument;
import com.example.Catalogo_Cursos_Indexer.event.CourseCreatedEvent;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Convierte el evento de dominio recibido desde Kafka al modelo
 * de lectura que se persiste en Elasticsearch.
 *
 * El documento conserva únicamente la información disponible
 * en el evento de Course.
 */
@Component
public class CourseDocumentMapper {

  public CourseDocument toDocument(CourseCreatedEvent event) {
    return new CourseDocument(
        event.courseId(),
        event.code(),
        event.name(),
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
