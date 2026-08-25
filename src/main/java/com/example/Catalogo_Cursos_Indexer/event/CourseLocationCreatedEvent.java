package com.example.Catalogo_Cursos_Indexer.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.UUID;

/**
 * Representa el payload desempaquetado por el Outbox Event Router
 * de Debezium.
 *
 * Corresponde al evento de creación/actualización de una ubicación
 * asociada a un curso.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CourseLocationCreatedEvent(
    UUID courseLocationId,
    UUID courseId,
    String name,
    String address,
    String city,
    Integer capacity,
    Instant createdAt,
    String reference) {
}
