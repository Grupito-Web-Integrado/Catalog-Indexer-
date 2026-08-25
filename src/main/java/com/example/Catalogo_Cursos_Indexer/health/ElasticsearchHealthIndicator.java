package com.example.Catalogo_Cursos_Indexer.health;

import co.elastic.clients.elasticsearch.ElasticsearchClient;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Verifica conectividad con el cluster de Elasticsearch usando el
 * endpoint de info, que es liviano y no requiere permisos especiales.
 */
@Component
public class ElasticsearchHealthIndicator implements HealthIndicator {

  private final ElasticsearchClient client;

  public ElasticsearchHealthIndicator(ElasticsearchClient client) {
    this.client = client;
  }

  @Override
  public Health health() {
    try {
      var info = client.info();
      return Health.up()
          .withDetail("clusterName", info.clusterName())
          .withDetail("version", info.version().number())
          .build();
    } catch (Exception e) {
      return Health.down()
          .withDetail("error", e.getMessage())
          .build();
    }
  }
}
