package com.example.Catalogo_Cursos_Indexer.consumer;

import com.example.Catalogo_Cursos_Indexer.event.CourseScheduleCreatedEvent;
import com.example.Catalogo_Cursos_Indexer.service.IndexingService;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class CourseScheduleConsumer {

  private static final Logger log = LoggerFactory.getLogger(CourseScheduleConsumer.class);

  private final IndexingService indexingService;
  private final ObjectMapper objectMapper;

  public CourseScheduleConsumer(
      IndexingService indexingService,
      ObjectMapper objectMapper) {
    this.indexingService = indexingService;
    this.objectMapper = objectMapper;
  }

  @KafkaListener(topics = "${catalog-indexer.kafka.topics.course-schedule:catalog.COURSE_SCHEDULE.events}", groupId = "${catalog-indexer.kafka.group-id:catalog-search-indexer}")
  public void onMessage(String payload) {

    try {

      String normalizedPayload = normalize(payload);

      CourseScheduleCreatedEvent event = objectMapper.readValue(
          normalizedPayload,
          CourseScheduleCreatedEvent.class);

      log.debug(
          "Evento CourseSchedule recibido: courseScheduleId={}",
          event.courseScheduleId());

      indexingService.handleCourseScheduleCreated(event);

    } catch (Exception e) {

      log.error(
          "Error deserializando o procesando evento CourseSchedule. " +
              "Payload: {}",
          payload,
          e);

      throw new RuntimeException(
          "Fallo procesando CourseScheduleCreatedEvent",
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
