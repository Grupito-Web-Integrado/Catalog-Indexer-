package com.example.Catalogo_Cursos_Indexer.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

/**
 * Representa el payload desempaquetado por el Outbox Event Router
 * de Debezium.
 *
 * Corresponde al evento de creación/actualización de un curso.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CourseCreatedEvent(
    UUID courseId,
    String code,
    String name,
    BigDecimal price,
    String status,
    LocalDate endDate,
    Integer capacity,
    String currency,
    String modality,
    Instant createdAt,
    LocalDate startDate,
    LocalTime startTime,
    String description,
    Integer durationHours,
    Integer availableSlots) {
}
