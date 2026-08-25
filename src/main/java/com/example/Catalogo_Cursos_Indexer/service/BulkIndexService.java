package com.example.Catalogo_Cursos_Indexer.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;

import com.example.Catalogo_Cursos_Indexer.dlq.DeadLetterPublisher;
import com.example.Catalogo_Cursos_Indexer.dlq.FailedEvent;
import com.example.Catalogo_Cursos_Indexer.document.CourseCategoryDocument;
import com.example.Catalogo_Cursos_Indexer.document.CourseDocument;
import com.example.Catalogo_Cursos_Indexer.document.CourseLocationDocument;
import com.example.Catalogo_Cursos_Indexer.document.CourseScheduleDocument;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Servicio responsable de indexar documentos del catálogo de cursos
 * utilizando la Bulk API de Elasticsearch.
 *
 * Índices manejados:
 *
 * - courses
 * - course_categories
 * - course_locations
 * - course_schedules
 *
 * La escritura en Elasticsearch se realiza exclusivamente mediante
 * la Bulk API.
 *
 * Si Elasticsearch no está disponible, el lote completo se reintenta
 * mediante RetryService.
 *
 * Si Elasticsearch acepta el bulk pero falla un documento individual,
 * solamente ese documento se envía al DLQ.
 */
@Service
public class BulkIndexService {

  private static final Logger log = LoggerFactory.getLogger(BulkIndexService.class);

  private static final String COURSES_INDEX = "courses";
  private static final String COURSE_CATEGORIES_INDEX = "course_categories";
  private static final String COURSE_LOCATIONS_INDEX = "course_locations";
  private static final String COURSE_SCHEDULES_INDEX = "course_schedules";

  private final ElasticsearchClient elasticsearchClient;
  private final RetryService retryService;
  private final DeadLetterPublisher deadLetterPublisher;
  private final ObjectMapper objectMapper;

  public BulkIndexService(
      ElasticsearchClient elasticsearchClient,
      RetryService retryService,
      DeadLetterPublisher deadLetterPublisher,
      ObjectMapper objectMapper) {
    this.elasticsearchClient = elasticsearchClient;
    this.retryService = retryService;
    this.deadLetterPublisher = deadLetterPublisher;
    this.objectMapper = objectMapper;
  }

  /**
   * Indexa cursos.
   */
  public void indexCourses(List<CourseDocument> documents) {

    indexBatch(
        COURSES_INDEX,
        documents,
        CourseDocument::courseId);
  }

  /**
   * Indexa categorías de cursos.
   */
  public void indexCourseCategories(
      List<CourseCategoryDocument> documents) {

    indexBatch(
        COURSE_CATEGORIES_INDEX,
        documents,
        CourseCategoryDocument::courseCategoryId);
  }

  /**
   * Indexa ubicaciones de cursos.
   */
  public void indexCourseLocations(
      List<CourseLocationDocument> documents) {

    indexBatch(
        COURSE_LOCATIONS_INDEX,
        documents,
        CourseLocationDocument::courseLocationId);
  }

  /**
   * Indexa horarios de cursos.
   */
  public void indexCourseSchedules(
      List<CourseScheduleDocument> documents) {

    indexBatch(
        COURSE_SCHEDULES_INDEX,
        documents,
        CourseScheduleDocument::courseScheduleId);
  }

  /**
   * Ejecuta la indexación genérica de un lote.
   */
  private <T> void indexBatch(
      String indexName,
      List<T> documents,
      Function<T, Object> idExtractor) {

    if (documents == null || documents.isEmpty()) {
      log.debug(
          "No existen documentos para indexar en [{}]",
          indexName);
      return;
    }

    log.info(
        "Indexando lote de {} documentos en índice [{}]",
        documents.size(),
        indexName);

    try {

      BulkResponse response = retryService.executeWithRetry(
          "bulk-index-" + indexName,
          () -> executeBulk(
              indexName,
              documents,
              idExtractor));

      handlePartialFailures(
          indexName,
          documents,
          idExtractor,
          response);

    } catch (RuntimeException e) {

      log.error(
          "El lote completo falló para índice [{}] " +
              "después de agotar los reintentos. " +
              "Enviando {} documentos al DLQ.",
          indexName,
          documents.size(),
          e);

      documents.forEach(document -> sendToDlq(
          indexName,
          document,
          idExtractor,
          e,
          retryService.maxAttempts()));
    }
  }

  /**
   * Ejecuta físicamente la operación Bulk contra Elasticsearch.
   */
  private <T> BulkResponse executeBulk(
      String indexName,
      List<T> documents,
      Function<T, Object> idExtractor) {

    try {

      BulkRequest.Builder builder = new BulkRequest.Builder();

      for (T document : documents) {

        String id = String.valueOf(
            idExtractor.apply(document));

        builder.operations(
            operation -> operation.index(
                indexOperation -> indexOperation
                    .index(indexName)
                    .id(id)
                    .document(document)));
      }

      return elasticsearchClient.bulk(
          builder.build());

    } catch (IOException e) {

      throw new ElasticsearchBulkException(
          "Error de IO ejecutando Bulk API " +
              "en índice [" + indexName + "]",
          e);
    }
  }

  /**
   * Detecta errores individuales dentro de una respuesta Bulk.
   *
   * Elasticsearch puede devolver una respuesta HTTP exitosa
   * aunque determinados documentos hayan fallado.
   *
   * En ese caso solamente se envía al DLQ el documento afectado.
   */
  private <T> void handlePartialFailures(
      String indexName,
      List<T> documents,
      Function<T, Object> idExtractor,
      BulkResponse response) {

    if (!response.errors()) {

      log.debug(
          "Lote indexado correctamente en [{}]. " +
              "Documentos: {}",
          indexName,
          documents.size());

      return;
    }

    Map<String, T> documentsById = documents.stream()
        .collect(
            Collectors.toMap(
                document -> String.valueOf(
                    idExtractor.apply(document)),
                document -> document));

    for (BulkResponseItem item : response.items()) {

      if (item.error() == null) {
        continue;
      }

      T failedDocument = documentsById.get(item.id());

      log.warn(
          "Documento falló durante Bulk API. " +
              "index={}, documentId={}, reason={}",
          indexName,
          item.id(),
          item.error().reason());

      sendToDlq(
          indexName,
          failedDocument,
          idExtractor,
          new ElasticsearchBulkException(
              item.error().reason(),
              null),
          1);
    }
  }

  /**
   * Publica un documento fallido en el DLQ.
   */
  private <T> void sendToDlq(
      String indexName,
      T document,
      Function<T, Object> idExtractor,
      Throwable error,
      int attemptCount) {

    if (document == null) {
      return;
    }

    try {

      String payloadJson = objectMapper.writeValueAsString(document);

      String documentId = String.valueOf(
          idExtractor.apply(document));

      FailedEvent failedEvent = FailedEvent.of(
          indexName,
          documentId,
          payloadJson,
          error,
          attemptCount);

      deadLetterPublisher.publish(
          failedEvent);

    } catch (Exception serializationError) {

      log.error(
          "No se pudo serializar el documento " +
              "fallido para DLQ. index={}",
          indexName,
          serializationError);
    }
  }
}
