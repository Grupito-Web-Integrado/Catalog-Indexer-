package com.example.Catalogo_Cursos_Indexer.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Acumula elementos en memoria y dispara un flush cuando ocurre
 * PRIMERO una de dos condiciones:
 * - se alcanza el tamaño máximo del lote (maxBatchSize)
 * - ha transcurrido el intervalo máximo de tiempo desde el último
 * flush (maxIntervalMillis), evaluado por el BatchScheduler externo
 *
 * Es agnóstico al tipo de documento (Book/Author/Category): cada
 * índice tiene su propia instancia de este acumulador.
 *
 * Thread-safety: el consumer de Kafka (poll-thread) y el scheduler
 * (thread de scheduling) acceden concurrentemente -> se protege con
 * un ReentrantLock explícito en vez de synchronized, porque el flush
 * puede tardar (llamada de red a Elasticsearch) y no queremos bloquear
 * el lock durante esa llamada -> se copia y limpia el buffer dentro
 * del lock, y se procesa la copia fuera del lock.
 */
public class BatchAccumulator<T> {

  private static final Logger log = LoggerFactory.getLogger(BatchAccumulator.class);

  private final String batchName;
  private final int maxBatchSize;
  private final Consumer<List<T>> onFlush;
  private final ReentrantLock lock = new ReentrantLock();

  private List<T> buffer;
  private volatile Instant lastFlushAt;

  public BatchAccumulator(String batchName, int maxBatchSize, Consumer<List<T>> onFlush) {
    this.batchName = batchName;
    this.maxBatchSize = maxBatchSize;
    this.onFlush = onFlush;
    this.buffer = new ArrayList<>(maxBatchSize);
    this.lastFlushAt = Instant.now();
  }

  /**
   * Agrega un elemento al buffer. Si al agregarlo se alcanza el
   * tamaño máximo, dispara el flush inmediatamente (disparo por tamaño).
   */
  public void add(T item) {
    List<T> toFlush = null;

    lock.lock();
    try {
      buffer.add(item);
      if (buffer.size() >= maxBatchSize) {
        toFlush = drainLocked();
      }
    } finally {
      lock.unlock();
    }

    if (toFlush != null) {
      log.debug("Batch [{}] alcanzó tamaño máximo ({}), disparando flush", batchName, maxBatchSize);
      flush(toFlush);
    }
  }

  /**
   * Invocado periódicamente por el BatchScheduler. Si hay elementos
   * pendientes, los vacía sin importar cuántos sean (disparo por tiempo).
   */
  public void flushIfPending() {
    List<T> toFlush = null;

    lock.lock();
    try {
      if (!buffer.isEmpty()) {
        toFlush = drainLocked();
      }
    } finally {
      lock.unlock();
    }

    if (toFlush != null) {
      log.debug("Batch [{}] flush por intervalo de tiempo, {} elementos pendientes", batchName, toFlush.size());
      flush(toFlush);
    }
  }

  /**
   * Debe llamarse con el lock ya adquirido. Reemplaza el buffer interno
   * por uno nuevo y devuelve la lista acumulada hasta el momento.
   */
  private List<T> drainLocked() {
    List<T> drained = buffer;
    buffer = new ArrayList<>(maxBatchSize);
    lastFlushAt = Instant.now();
    return drained;
  }

  private void flush(List<T> items) {
    try {
      onFlush.accept(items);
    } catch (Exception e) {
      log.error("Error procesando flush del batch [{}] con {} elementos", batchName, items.size(), e);
    }
  }

  public int pendingCount() {
    lock.lock();
    try {
      return buffer.size();
    } finally {
      lock.unlock();
    }
  }

  public Instant lastFlushAt() {
    return lastFlushAt;
  }
}
