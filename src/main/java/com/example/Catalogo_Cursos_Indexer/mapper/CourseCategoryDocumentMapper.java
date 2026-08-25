package com.example.Catalogo_Cursos_Indexer.mapper;

import com.example.Catalogo_Cursos_Indexer.document.CourseCategoryDocument;
import com.example.Catalogo_Cursos_Indexer.event.CourseCategoryCreatedEvent;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Convierte el evento de categoría recibido desde Kafka
 * al documento de lectura para Elasticsearch.
 */
@Component
public class CourseCategoryDocumentMapper {

  public CourseCategoryDocument toDocument(
      CourseCategoryCreatedEvent event) {
    return new CourseCategoryDocument(
        event.courseCategoryId(),
        event.name(),
        event.status(),
        event.description(),
        event.createdAt(),
        Instant.now());
  }
}
