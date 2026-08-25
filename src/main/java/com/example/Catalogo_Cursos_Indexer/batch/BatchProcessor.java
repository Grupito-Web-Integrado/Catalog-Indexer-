package com.example.Catalogo_Cursos_Indexer.batch;

import com.example.Catalogo_Cursos_Indexer.document.CourseCategoryDocument;
import com.example.Catalogo_Cursos_Indexer.document.CourseDocument;
import com.example.Catalogo_Cursos_Indexer.document.CourseLocationDocument;
import com.example.Catalogo_Cursos_Indexer.document.CourseScheduleDocument;
import com.example.Catalogo_Cursos_Indexer.service.BulkIndexService;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Punto único de acceso a los acumuladores de batch.
 *
 * Cada consumer de Kafka recibe un evento y, a través de
 * IndexingService, entrega el documento correspondiente a uno
 * de estos acumuladores.
 *
 * El BatchProcessor NO escribe directamente en Elasticsearch.
 *
 * Flujo:
 *
 * Kafka
 * ↓
 * Consumer
 * ↓
 * IndexingService
 * ↓
 * Mapper
 * ↓
 * BatchProcessor
 * ↓
 * BatchAccumulator
 * ↓
 * BulkIndexService
 * ↓
 * Elasticsearch
 *
 * Cada tipo de documento tiene su propio buffer.
 */
@Component
public class BatchProcessor {

  private final BatchAccumulator<CourseDocument> courseAccumulator;

  private final BatchAccumulator<CourseCategoryDocument> courseCategoryAccumulator;

  private final BatchAccumulator<CourseLocationDocument> courseLocationAccumulator;

  private final BatchAccumulator<CourseScheduleDocument> courseScheduleAccumulator;

  public BatchProcessor(
      BulkIndexService bulkIndexService,
      @Value("${catalog-indexer.batch.max-size:500}") int maxBatchSize) {

    /*
     * =====================================================
     * COURSES
     * =====================================================
     */
    this.courseAccumulator = new BatchAccumulator<>(
        "courses",
        maxBatchSize,
        bulkIndexService::indexCourses);

    /*
     * =====================================================
     * COURSE CATEGORIES
     * =====================================================
     */
    this.courseCategoryAccumulator = new BatchAccumulator<>(
        "course_categories",
        maxBatchSize,
        bulkIndexService::indexCourseCategories);

    /*
     * =====================================================
     * COURSE LOCATIONS
     * =====================================================
     */
    this.courseLocationAccumulator = new BatchAccumulator<>(
        "course_locations",
        maxBatchSize,
        bulkIndexService::indexCourseLocations);

    /*
     * =====================================================
     * COURSE SCHEDULES
     * =====================================================
     */
    this.courseScheduleAccumulator = new BatchAccumulator<>(
        "course_schedules",
        maxBatchSize,
        bulkIndexService::indexCourseSchedules);
  }

  /**
   * Acumula un documento de curso.
   */
  public void accumulateCourse(
      CourseDocument document) {

    courseAccumulator.add(document);
  }

  /**
   * Acumula un documento de categoría.
   */
  public void accumulateCourseCategory(
      CourseCategoryDocument document) {

    courseCategoryAccumulator.add(document);
  }

  /**
   * Acumula un documento de ubicación.
   */
  public void accumulateCourseLocation(
      CourseLocationDocument document) {

    courseLocationAccumulator.add(document);
  }

  /**
   * Acumula un documento de horario.
   */
  public void accumulateCourseSchedule(
      CourseScheduleDocument document) {

    courseScheduleAccumulator.add(document);
  }

  /**
   * Vacía todos los acumuladores que tengan documentos pendientes.
   *
   * Este método es invocado periódicamente por BatchScheduler.
   *
   * No importa si el acumulador alcanzó maxBatchSize:
   * si tiene documentos pendientes, se envían.
   */
  public void flushAllPending() {

    courseAccumulator.flushIfPending();

    courseCategoryAccumulator.flushIfPending();

    courseLocationAccumulator.flushIfPending();

    courseScheduleAccumulator.flushIfPending();
  }

  /**
   * Devuelve la cantidad de documentos pendientes en cada
   * acumulador.
   *
   * Orden:
   *
   * [courses,
   * course_categories,
   * course_locations,
   * course_schedules]
   */
  public List<Integer> pendingCounts() {

    return List.of(
        courseAccumulator.pendingCount(),
        courseCategoryAccumulator.pendingCount(),
        courseLocationAccumulator.pendingCount(),
        courseScheduleAccumulator.pendingCount());
  }
}
