package com.example.Catalogo_Cursos_Indexer.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.UUID;

/**
 * Representa el payload desempaquetado por el Outbox Event Router
 * de Debezium.
 *
 * Corresponde al evento de creación/actualización de una categoría.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CourseCategoryCreatedEvent(
    UUID courseCategoryId,
    String name,
    String status,
    Instant createdAt,
    String description) {
}
