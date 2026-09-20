package com.example.Catalogo_Cursos_Indexer.consumer;

import com.example.Catalogo_Cursos_Indexer.event.CourseCreatedEvent;
import com.example.Catalogo_Cursos_Indexer.event.course.CourseUpdateEvent;
import com.example.Catalogo_Cursos_Indexer.service.IndexingService;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.consumer.ConsumerRecord;
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
  public void onMessage(
      ConsumerRecord<String, String> record) {

    try {

      String payload = record.value();

      String eventType = getHeader(
          record,
          "eventType");

      log.debug(
          "Evento Course recibido: eventType={}, eventId={}, key={}",
          eventType,
          getHeader(record, "eventId"),
          record.key());

      String normalizedPayload = normalize(payload);

      switch (eventType) {

        case "COURSE_CREATED" -> {

          CourseCreatedEvent event = objectMapper.readValue(
              normalizedPayload,
              CourseCreatedEvent.class);

          log.info(
              "Procesando CourseCreatedEvent: courseId={}",
              event.courseId());

          indexingService.handleCourseCreated(event);
        }

        case "COURSE_UPDATED" -> {

          CourseUpdateEvent event = objectMapper.readValue(
              normalizedPayload,
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
          "Error deserializando o procesando evento Course. " +
              "topic={}, partition={}, offset={}, payload={}",
          record.topic(),
          record.partition(),
          record.offset(),
          record.value(),
          e);

      throw new RuntimeException(
          "Fallo procesando evento Course",
          e);
    }
  }

  private String getHeader(
      ConsumerRecord<String, String> record,
      String headerName) {

    var header = record.headers().lastHeader(headerName);

    if (header == null) {
      return null;
    }

    return new String(
        header.value(),
        java.nio.charset.StandardCharsets.UTF_8);
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
