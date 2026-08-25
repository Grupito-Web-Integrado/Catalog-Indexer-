package com.example.Catalogo_Cursos_Indexer.mapper;

import com.example.Catalogo_Cursos_Indexer.document.CourseLocationDocument;
import com.example.Catalogo_Cursos_Indexer.event.CourseLocationCreatedEvent;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Convierte el evento de ubicación recibido desde Kafka
 * al documento de lectura para Elasticsearch.
 */
@Component
public class CourseLocationDocumentMapper {

  public CourseLocationDocument toDocument(
      CourseLocationCreatedEvent event) {
    return new CourseLocationDocument(
        event.courseLocationId(),
        event.courseId(),
        event.name(),
        event.address(),
        event.city(),
        event.reference(),
        event.capacity(),
        event.createdAt(),
        Instant.now());
  }
}
