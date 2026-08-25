package com.example.Catalogo_Cursos_Indexer.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.example.Catalogo_Cursos_Indexer.document.CourseLocationDocument;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.UUID;

/**
 * Acceso puntual al índice "course_locations".
 *
 * La escritura masiva es responsabilidad de BulkIndexService.
 */
@Repository
public class CourseLocationIndexRepository {

  private static final String INDEX = "course_locations";

  private final ElasticsearchClient client;

  public CourseLocationIndexRepository(ElasticsearchClient client) {
    this.client = client;
  }

  public boolean exists(UUID courseLocationId) throws IOException {
    return client
        .exists(e -> e
            .index(INDEX)
            .id(courseLocationId.toString()))
        .value();
  }

  public CourseLocationDocument findById(
      UUID courseLocationId) throws IOException {

    var response = client.get(
        g -> g
            .index(INDEX)
            .id(courseLocationId.toString()),
        CourseLocationDocument.class);

    return response.found()
        ? response.source()
        : null;
  }
}
