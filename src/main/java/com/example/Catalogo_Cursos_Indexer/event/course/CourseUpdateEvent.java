package com.example.Catalogo_Cursos_Indexer.event.course;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

public record CourseUpdateEvent(
    UUID courseId,
    String code,
    String name,
    String description,
    String imageKey,
    String modality,
    BigDecimal price,
    String currency,
    LocalDate startDate,
    LocalDate endDate,
    LocalTime startTime,
    Integer durationHours,
    Integer capacity,
    Integer availableSlots,
    UUID categoryId,
    String status,
    Instant updatedAt) {
}
