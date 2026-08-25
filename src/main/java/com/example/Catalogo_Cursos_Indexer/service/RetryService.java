package com.example.Catalogo_Cursos_Indexer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.function.Supplier;

/**
 * Reintenta una operación (normalmente una llamada bulk a Elasticsearch)
 * con backoff exponencial simple. Si se agotan los intentos, relanza
 * la última excepción para que el caller decida qué hacer (en este
 * proyecto: publicar al DLQ vía DeadLetterPublisher).
 */
@Component
public class RetryService {

  private static final Logger log = LoggerFactory.getLogger(RetryService.class);

  private final int maxAttempts;
  private final long initialBackoffMillis;

  public RetryService(
      @Value("${catalog-indexer.retry.max-attempts:3}") int maxAttempts,
      @Value("${catalog-indexer.retry.initial-backoff-millis:200}") long initialBackoffMillis) {
    this.maxAttempts = maxAttempts;
    this.initialBackoffMillis = initialBackoffMillis;
  }

  /**
   * Ejecuta la operación dada. Si lanza excepción, reintenta hasta
   * maxAttempts veces con backoff exponencial (initialBackoffMillis,
   * x2, x4, ...). Si el último intento también falla, propaga la
   * excepción original al caller.
   */
  public <T> T executeWithRetry(String operationName, Supplier<T> operation) {
    RuntimeException lastError = null;

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        return operation.get();
      } catch (RuntimeException e) {
        lastError = e;
        log.warn(
            "Intento {}/{} fallido para operación [{}]: {}",
            attempt, maxAttempts, operationName, e.getMessage());

        if (attempt < maxAttempts) {
          sleepBackoff(attempt);
        }
      }
    }

    log.error("Operación [{}] agotó {} intentos, propagando error", operationName, maxAttempts);
    throw lastError;
  }

  private void sleepBackoff(int attempt) {
    long backoffMillis = initialBackoffMillis * (1L << (attempt - 1));
    try {
      Thread.sleep(backoffMillis);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Retry interrumpido", ie);
    }
  }

  public int maxAttempts() {
    return maxAttempts;
  }
}
