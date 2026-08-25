package com.example.Catalogo_Cursos_Indexer.dlq;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publica al tópico dead-letter cualquier documento que no pudo
 * indexarse en Elasticsearch tras agotar los reintentos configurados
 * en RetryService.
 *
 * El topic destino es configurable porque conviene tener uno por
 * índice (books-dlq, authors-dlq, categories-dlq) o uno centralizado,
 * según cómo quieras operar el reprocesamiento manual después.
 */
@Component
public class DeadLetterPublisher {

  private static final Logger log = LoggerFactory.getLogger(DeadLetterPublisher.class);

  private final KafkaTemplate<String, String> kafkaTemplate;
  private final ObjectMapper objectMapper;
  private final String dlqTopic;

  public DeadLetterPublisher(
      KafkaTemplate<String, String> kafkaTemplate,
      ObjectMapper objectMapper,
      @Value("${catalog-indexer.kafka.dlq-topic:catalog.indexer.dlq}") String dlqTopic) {
    this.kafkaTemplate = kafkaTemplate;
    this.objectMapper = objectMapper;
    this.dlqTopic = dlqTopic;
  }

  public void publish(FailedEvent failedEvent) {
    try {
      String json = objectMapper.writeValueAsString(failedEvent);
      kafkaTemplate.send(dlqTopic, failedEvent.documentId(), json);
      log.warn(
          "Evento enviado a DLQ [{}] - index={} documentId={} error={}",
          dlqTopic, failedEvent.sourceIndex(), failedEvent.documentId(), failedEvent.errorMessage());
    } catch (Exception e) {
      log.error(
          "Fallo crítico: no se pudo publicar al DLQ el documentId={}. El evento se pierde.",
          failedEvent.documentId(), e);
    }
  }
}
