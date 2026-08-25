package com.example.Catalogo_Cursos_Indexer.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.example.Catalogo_Cursos_Indexer.document.CourseCategoryDocument;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.UUID;

/**
 * Acceso puntual al índice "course_categories".
 *
 * La escritura masiva es responsabilidad de BulkIndexService.
 */
@Repository
public class CourseCategoryIndexRepository {

  private static final String INDEX = "course_categories";

  private final ElasticsearchClient client;

  public CourseCategoryIndexRepository(ElasticsearchClient client) {
    this.client = client;
  }

  public boolean exists(UUID courseCategoryId) throws IOException {
    return client
        .exists(e -> e
            .index(INDEX)
            .id(courseCategoryId.toString()))
        .value();
  }

  public CourseCategoryDocument findById(
      UUID courseCategoryId) throws IOException {

    var response = client.get(
        g -> g
            .index(INDEX)
            .id(courseCategoryId.toString()),
        CourseCategoryDocument.class);

    return response.found()
        ? response.source()
        : null;
  }
}
