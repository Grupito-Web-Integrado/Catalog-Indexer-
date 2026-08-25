package com.example.Catalogo_Cursos_Indexer.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Disparador por tiempo del patrón Batch: cada fixedRateMillis,
 * fuerza el vaciado de cualquier acumulador con elementos pendientes,
 * sin importar si llegó a su tamaño máximo.
 *
 * Esto garantiza una latencia máxima de indexación aunque el tráfico
 * de eventos sea bajo (ej. solo 3 libros creados en la última hora
 * no se quedan esperando indefinidamente a que se junten 500).
 */
@Component
public class BatchScheduler {

  private static final Logger log = LoggerFactory.getLogger(BatchScheduler.class);

  private final BatchProcessor batchProcessor;

  public BatchScheduler(BatchProcessor batchProcessor) {
    this.batchProcessor = batchProcessor;
  }

  @Scheduled(fixedRateString = "${catalog-indexer.batch.max-interval-millis:5000}")
  public void flushPendingBatches() {
    log.trace("Tick de BatchScheduler, pendientes: {}", batchProcessor.pendingCounts());
    batchProcessor.flushAllPending();
  }
}
