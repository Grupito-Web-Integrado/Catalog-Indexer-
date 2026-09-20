package com.example.Catalogo_Cursos_Indexer.consumer;

import com.example.Catalogo_Cursos_Indexer.event.CourseCreatedEvent;
import com.example.Catalogo_Cursos_Indexer.event.course.CourseUpdateEvent;
import com.example.Catalogo_Cursos_Indexer.service.IndexingService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

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

      JsonNode json = objectMapper.readTree(normalizedPayload);

      String eventType = json.path("eventType").asText();

      log.debug(
          "Evento Course recibido: eventType={}",
          eventType);

      switch (eventType) {

        case "COURSE_CREATED" -> {

          CourseCreatedEvent event = objectMapper.treeToValue(
              json,
              CourseCreatedEvent.class);

          log.info(
              "Procesando CourseCreatedEvent: courseId={}",
              event.courseId());

          indexingService.handleCourseCreated(event);
        }

        case "COURSE_UPDATED" -> {

          CourseUpdateEvent event = objectMapper.treeToValue(
              json,
              CourseUpdateEvent.class);

          log.info(
              "Procesando CourseUpdateEvent: courseId={}",
              event.courseId());

          indexingService.handleCourseUpdated(event);
        }

        default -> {

          log.warn(
              "Evento Course no soportado: eventType={}",
              eventType);
        }
      }

    } catch (Exception e) {

      log.error(
          "Error deserializando o procesando evento Course. Payload: {}",
          payload,
          e);

      throw new RuntimeException(
          "Fallo procesando evento Course",
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
