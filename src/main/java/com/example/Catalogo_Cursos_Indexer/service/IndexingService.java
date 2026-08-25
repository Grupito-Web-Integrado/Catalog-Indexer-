package com.example.Catalogo_Cursos_Indexer.service;

import com.example.Catalogo_Cursos_Indexer.batch.BatchProcessor;
import com.example.Catalogo_Cursos_Indexer.event.CourseCategoryCreatedEvent;
import com.example.Catalogo_Cursos_Indexer.event.CourseCreatedEvent;
import com.example.Catalogo_Cursos_Indexer.event.CourseLocationCreatedEvent;
import com.example.Catalogo_Cursos_Indexer.event.CourseScheduleCreatedEvent;
import com.example.Catalogo_Cursos_Indexer.mapper.CourseCategoryDocumentMapper;
import com.example.Catalogo_Cursos_Indexer.mapper.CourseDocumentMapper;
import com.example.Catalogo_Cursos_Indexer.mapper.CourseLocationDocumentMapper;
import com.example.Catalogo_Cursos_Indexer.mapper.CourseScheduleDocumentMapper;
import org.springframework.stereotype.Service;

/**
 * Fachada utilizada por los consumers de Kafka.
 *
 * Responsabilidades:
 *
 * 1. Recibir el evento de dominio.
 * 2. Convertir el evento a su Document correspondiente.
 * 3. Entregar el documento al BatchProcessor.
 *
 * Este servicio NO realiza I/O contra Elasticsearch.
 *
 * El flujo de escritura es:
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
 * BulkIndexService
 * ↓
 * Elasticsearch
 */
@Service
public class IndexingService {

  private final BatchProcessor batchProcessor;

  private final CourseDocumentMapper courseDocumentMapper;

  private final CourseCategoryDocumentMapper courseCategoryDocumentMapper;

  private final CourseLocationDocumentMapper courseLocationDocumentMapper;

  private final CourseScheduleDocumentMapper courseScheduleDocumentMapper;

  public IndexingService(
      BatchProcessor batchProcessor,
      CourseDocumentMapper courseDocumentMapper,
      CourseCategoryDocumentMapper courseCategoryDocumentMapper,
      CourseLocationDocumentMapper courseLocationDocumentMapper,
      CourseScheduleDocumentMapper courseScheduleDocumentMapper) {
    this.batchProcessor = batchProcessor;
    this.courseDocumentMapper = courseDocumentMapper;
    this.courseCategoryDocumentMapper = courseCategoryDocumentMapper;
    this.courseLocationDocumentMapper = courseLocationDocumentMapper;
    this.courseScheduleDocumentMapper = courseScheduleDocumentMapper;
  }

  /**
   * Procesa un evento de creación/actualización de curso.
   */
  public void handleCourseCreated(
      CourseCreatedEvent event) {

    batchProcessor.accumulateCourse(
        courseDocumentMapper.toDocument(event));
  }

  /**
   * Procesa un evento de categoría de curso.
   */
  public void handleCourseCategoryCreated(
      CourseCategoryCreatedEvent event) {

    batchProcessor.accumulateCourseCategory(
        courseCategoryDocumentMapper.toDocument(event));
  }

  /**
   * Procesa un evento de ubicación del curso.
   */
  public void handleCourseLocationCreated(
      CourseLocationCreatedEvent event) {

    batchProcessor.accumulateCourseLocation(
        courseLocationDocumentMapper.toDocument(event));
  }

  /**
   * Procesa un evento de horario del curso.
   */
  public void handleCourseScheduleCreated(
      CourseScheduleCreatedEvent event) {

    batchProcessor.accumulateCourseSchedule(
        courseScheduleDocumentMapper.toDocument(event));
  }
}
