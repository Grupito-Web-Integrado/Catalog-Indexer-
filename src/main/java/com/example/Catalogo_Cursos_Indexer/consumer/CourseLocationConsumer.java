package com.example.Catalogo_Cursos_Indexer.consumer;

import com.example.Catalogo_Cursos_Indexer.event.CourseLocationCreatedEvent;
import com.example.Catalogo_Cursos_Indexer.service.IndexingService;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class CourseLocationConsumer {

  private static final Logger log = LoggerFactory.getLogger(CourseLocationConsumer.class);

  private final IndexingService indexingService;
  private final ObjectMapper objectMapper;

  public CourseLocationConsumer(
      IndexingService indexingService,
      ObjectMapper objectMapper) {
    this.indexingService = indexingService;
    this.objectMapper = objectMapper;
  }

  @KafkaListener(topics = "${catalog-indexer.kafka.topics.course-location:catalog.COURSE_LOCATION.events}", groupId = "${catalog-indexer.kafka.group-id:catalog-search-indexer}")
  public void onMessage(String payload) {

    try {

      String normalizedPayload = normalize(payload);

      CourseLocationCreatedEvent event = objectMapper.readValue(
          normalizedPayload,
          CourseLocationCreatedEvent.class);

      log.debug(
          "Evento CourseLocation recibido: courseLocationId={}",
          event.courseLocationId());

      indexingService.handleCourseLocationCreated(event);

    } catch (Exception e) {

      log.error(
          "Error deserializando o procesando evento CourseLocation. " +
              "Payload: {}",
          payload,
          e);

      throw new RuntimeException(
          "Fallo procesando CourseLocationCreatedEvent",
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
