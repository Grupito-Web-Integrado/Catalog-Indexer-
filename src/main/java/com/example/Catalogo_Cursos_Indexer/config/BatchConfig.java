package com.example.Catalogo_Cursos_Indexer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Habilita @Scheduled (requerido por BatchScheduler) y centraliza el
 * ObjectMapper usado por consumers, mappers y BulkIndexService para
 * que todos compartan la misma configuración (soporte de Instant/UUID).
 */
@EnableScheduling
@Configuration
public class BatchConfig {

  @Bean
  public ObjectMapper objectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    return mapper;
  }
}
