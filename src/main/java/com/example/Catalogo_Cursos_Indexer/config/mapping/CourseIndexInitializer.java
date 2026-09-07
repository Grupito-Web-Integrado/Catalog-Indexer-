package com.example.Catalogo_Cursos_Indexer.config.mapping;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class CourseIndexInitializer implements CommandLineRunner {
  private static final String INDEX = "courses";
  private final ElasticsearchClient elasticsearchClient;

  public CourseIndexInitializer(ElasticsearchClient elasticsearchClient) {
    this.elasticsearchClient = elasticsearchClient;
  }

  @Override
  public void run(String... args) throws Exception {
    boolean exists = elasticsearchClient.indices().exists(e -> e.index(INDEX)).value();
    if (exists) {
      return;
    }
    CreateIndexRequest request = CreateIndexRequest.of(create -> create.index(INDEX)
        .mappings(mapping -> mapping.properties("courseId", property -> property.keyword(keyword -> keyword))
            .properties("code",
                property -> property.text(text -> text.fields("keyword", field -> field.keyword(keyword -> keyword))))
            .properties("name", property -> property.completion(completion -> completion))
            .properties("status", property -> property.keyword(keyword -> keyword))
            .properties("price", property -> property.double_(doubleType -> doubleType))
            .properties("currency", property -> property.keyword(keyword -> keyword))
            .properties("modality", property -> property.keyword(keyword -> keyword))
            .properties("startDate", property -> property.date(date -> date))
            .properties("endDate", property -> property.date(date -> date))
            .properties("startTime", property -> property.keyword(keyword -> keyword))
            .properties("durationHours", property -> property.integer(integer -> integer))
            .properties("capacity", property -> property.integer(integer -> integer))
            .properties("availableSlots", property -> property.integer(integer -> integer))
            .properties("description", property -> property.text(text -> text))
            .properties("createdAt", property -> property.date(date -> date))
            .properties("indexedAt", property -> property.date(date -> date))));
    elasticsearchClient.indices().create(request);
  }
}
