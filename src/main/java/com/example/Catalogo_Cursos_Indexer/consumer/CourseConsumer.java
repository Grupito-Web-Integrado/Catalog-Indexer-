package com.example.Catalogo_Cursos_Indexer.consumer;

import com.example.Catalogo_Cursos_Indexer.event.CourseCreatedEvent;
import com.example.Catalogo_Cursos_Indexer.service.IndexingService;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consume eventos de Course publicados por Debezium Outbox Event Router.
 *
 * Topic:
 *
 * catalog.course.events
 *
 * El consumer no realiza I/O contra Elasticsearch.
 * Delega el procesamiento al IndexingService.
 */
@Component
public class CourseConsumer {

  private static final Logger log = LoggerFactory.getLogger(CourseConsumer.class);

  private final IndexingService indexingService;
  private final ObjectMapper objectMapper;

  public CourseConsumer(
      IndexingService indexingService,
      ObjectMapper objectMapper) {
    this.indexingService = indexingService;
    this.objectMapper = objectMapper;
  }

  @KafkaListener(topics = "${catalog-indexer.kafka.topics.course:catalog.COURSE.events}", groupId = "${catalog-indexer.kafka.group-id:catalog-search-indexer}")
  public void onMessage(String payload) {

    try {

      String normalizedPayload = normalize(payload);

      CourseCreatedEvent event = objectMapper.readValue(
          normalizedPayload,
          CourseCreatedEvent.class);

      log.debug(
          "Evento Course recibido: courseId={}",
          event.courseId());

      indexingService.handleCourseCreated(event);

    } catch (Exception e) {

      log.error(
          "Error deserializando o procesando evento Course. " +
              "Payload: {}",
          payload,
          e);

      throw new RuntimeException(
          "Fallo procesando CourseCreatedEvent",
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
