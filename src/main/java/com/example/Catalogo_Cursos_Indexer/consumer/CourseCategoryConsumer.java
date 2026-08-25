package com.example.Catalogo_Cursos_Indexer.consumer;

import com.example.Catalogo_Cursos_Indexer.event.CourseCategoryCreatedEvent;
import com.example.Catalogo_Cursos_Indexer.service.IndexingService;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class CourseCategoryConsumer {

  private static final Logger log = LoggerFactory.getLogger(CourseCategoryConsumer.class);

  private final IndexingService indexingService;
  private final ObjectMapper objectMapper;

  public CourseCategoryConsumer(
      IndexingService indexingService,
      ObjectMapper objectMapper) {
    this.indexingService = indexingService;
    this.objectMapper = objectMapper;
  }

  @KafkaListener(topics = "${catalog-indexer.kafka.topics.course-category:catalog.COURSE_CATEGORY.events}", groupId = "${catalog-indexer.kafka.group-id:catalog-search-indexer}")
  public void onMessage(String payload) {

    try {

      String normalizedPayload = normalize(payload);

      CourseCategoryCreatedEvent event = objectMapper.readValue(
          normalizedPayload,
          CourseCategoryCreatedEvent.class);

      log.debug(
          "Evento CourseCategory recibido: courseCategoryId={}",
          event.courseCategoryId());

      indexingService.handleCourseCategoryCreated(event);

    } catch (Exception e) {

      log.error(
          "Error deserializando o procesando evento CourseCategory. " +
              "Payload: {}",
          payload,
          e);

      throw new RuntimeException(
          "Fallo procesando CourseCategoryCreatedEvent",
          e);
    }
  }

  private String normalize(String payload)
      throws Exception {

    if (payload == null) {
      return null;
    }

    String trimmed = payload.trim();

    if (trimmed.startsWith("\"")) {
      return objectMapper.readValue(
          trimmed,
          String.class);
    }

    return trimmed;
  }
}
