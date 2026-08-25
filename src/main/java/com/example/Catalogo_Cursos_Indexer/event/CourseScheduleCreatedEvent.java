package com.example.Catalogo_Cursos_Indexer.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.time.LocalTime;
import java.util.UUID;

/**
 * Representa el payload desempaquetado por el Outbox Event Router
 * de Debezium.
 *
 * Corresponde al evento de creación/actualización del horario
 * de un curso.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CourseScheduleCreatedEvent(
    UUID courseScheduleId,
    UUID courseId,
    String dayOfWeek,
    LocalTime startTime,
    LocalTime endTime,
    String room,
    Instant createdAt) {
}
