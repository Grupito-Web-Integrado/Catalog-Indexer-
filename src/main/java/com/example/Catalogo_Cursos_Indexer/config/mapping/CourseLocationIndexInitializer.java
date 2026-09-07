package com.example.Catalogo_Cursos_Indexer.config.mapping;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class CourseLocationIndexInitializer implements CommandLineRunner {

  private static final String INDEX = "course_locations";

  private final ElasticsearchClient elasticsearchClient;

  public CourseLocationIndexInitializer(
      ElasticsearchClient elasticsearchClient) {

    this.elasticsearchClient = elasticsearchClient;
  }

  @Override
  public void run(String... args) throws Exception {

    boolean exists = elasticsearchClient
        .indices()
        .exists(e -> e.index(INDEX))
        .value();

    if (exists) {
      return;
    }

    CreateIndexRequest request = CreateIndexRequest.of(create -> create
        .index(INDEX)
        .mappings(mapping -> mapping
            .properties(
                "courseLocationId",
                property -> property.keyword(
                    keyword -> keyword))
            .properties(
                "courseId",
                property -> property.keyword(
                    keyword -> keyword))
            .properties(
                "name",
                property -> property.text(
                    text -> text.fields(
                        "keyword",
                        field -> field.keyword(
                            keyword -> keyword))))
            .properties(
                "address",
                property -> property.text(
                    text -> text.fields(
                        "keyword",
                        field -> field.keyword(
                            keyword -> keyword))))
            .properties(
                "city",
                property -> property.text(
                    text -> text.fields(
                        "keyword",
                        field -> field.keyword(
                            keyword -> keyword))))
            .properties(
                "reference",
                property -> property.text(
                    text -> text.fields(
                        "keyword",
                        field -> field.keyword(
                            keyword -> keyword))))
            .properties(
                "capacity",
                property -> property.integer(
                    integer -> integer))
            .properties(
                "createdAt",
                property -> property.date(
                    date -> date))
            .properties(
                "indexedAt",
                property -> property.date(
                    date -> date))));

    elasticsearchClient.indices().create(request);
  }
}
