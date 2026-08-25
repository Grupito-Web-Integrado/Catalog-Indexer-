package com.example.Catalogo_Cursos_Indexer.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.example.Catalogo_Cursos_Indexer.document.CourseDocument;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.UUID;

/**
 * Acceso puntual al índice "courses".
 *
 * La escritura en batch NO pasa por este repositorio.
 * La escritura es responsabilidad de BulkIndexService.
 *
 * Este repositorio se utiliza para:
 * - health checks
 * - verificaciones post-indexación
 * - consultas puntuales
 * - debugging
 */
@Repository
public class CourseIndexRepository {

  private static final String INDEX = "courses";

  private final ElasticsearchClient client;

  public CourseIndexRepository(ElasticsearchClient client) {
    this.client = client;
  }

  public boolean exists(UUID courseId) throws IOException {
    return client
        .exists(e -> e
            .index(INDEX)
            .id(courseId.toString()))
        .value();
  }

  public CourseDocument findById(UUID courseId) throws IOException {

    var response = client.get(
        g -> g
            .index(INDEX)
            .id(courseId.toString()),
        CourseDocument.class);

    return response.found()
        ? response.source()
        : null;
  }
}
