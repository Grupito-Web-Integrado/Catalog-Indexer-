package com.example.Catalogo_Cursos_Indexer.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.example.Catalogo_Cursos_Indexer.document.CourseScheduleDocument;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.UUID;

/**
 * Acceso puntual al índice "course_schedules".
 *
 * La escritura masiva es responsabilidad de BulkIndexService.
 */
@Repository
public class CourseScheduleIndexRepository {

  private static final String INDEX = "course_schedules";

  private final ElasticsearchClient client;

  public CourseScheduleIndexRepository(ElasticsearchClient client) {
    this.client = client;
  }

  public boolean exists(UUID courseScheduleId) throws IOException {
    return client
        .exists(e -> e
            .index(INDEX)
            .id(courseScheduleId.toString()))
        .value();
  }

  public CourseScheduleDocument findById(
      UUID courseScheduleId) throws IOException {

    var response = client.get(
        g -> g
            .index(INDEX)
            .id(courseScheduleId.toString()),
        CourseScheduleDocument.class);

    return response.found()
        ? response.source()
        : null;
  }
}
