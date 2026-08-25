package com.example.Catalogo_Cursos_Indexer.health;

import org.apache.kafka.clients.admin.AdminClient;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Verifica conectividad con el broker de Kafka consultando metadata
 * básica vía AdminClient, con timeout corto para no bloquear el
 * endpoint /actuator/health si el broker está caído.
 */
@Component
public class KafkaHealthIndicator implements HealthIndicator {

  private final KafkaAdmin kafkaAdmin;

  public KafkaHealthIndicator(KafkaAdmin kafkaAdmin) {
    this.kafkaAdmin = kafkaAdmin;
  }

  @Override
  public Health health() {
    try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
      var clusterId = adminClient.describeCluster().clusterId().get(3, TimeUnit.SECONDS);
      return Health.up()
          .withDetail("clusterId", clusterId)
          .build();
    } catch (Exception e) {
      return Health.down()
          .withDetail("error", e.getMessage())
          .build();
    }
  }
}
