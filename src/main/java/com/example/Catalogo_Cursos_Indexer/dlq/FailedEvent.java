package com.example.Catalogo_Cursos_Indexer.dlq;

import java.time.Instant;

/**
 * Envoltorio que se publica al tópico DLQ cuando un documento falla
 * al indexarse en Elasticsearch después de agotar los reintentos.
 *
 * originalPayload se mantiene como String (JSON ya serializado) para
 * no acoplar el DLQ a las clases de dominio concretas (Book/Author/
 * Category) y para que el mensaje sea autocontenible y legible por
 * cualquier consumidor, incluyendo herramientas de inspección manual.
 */
public record FailedEvent(
    String sourceIndex,
    String documentId,
    String originalPayload,
    String errorMessage,
    int attemptCount,
    Instant failedAt) {
  public static FailedEvent of(
      String sourceIndex,
      String documentId,
      String originalPayload,
      Throwable error,
      int attemptCount) {
    return new FailedEvent(
        sourceIndex,
        documentId,
        originalPayload,
        error.getMessage(),
        attemptCount,
        Instant.now());
  }
}
