package com.example.Catalogo_Cursos_Indexer.config;

import org.springframework.context.annotation.Configuration;

/**
 * Stub de configuración de observabilidad.
 *
 * Por ahora vacío a propósito: la instrumentación detallada (métricas
 * Micrometer por batch, spans de OpenTelemetry por operación bulk,
 * correlación con Jaeger/Loki) se añadirá en una iteración posterior.
 *
 * Cuando se active, este es el lugar para:
 * - Bean de MeterRegistry customizations (tags comunes: service.name)
 * - Bean de ObservationRegistry si se usa Micrometer Observation API
 * - Configuración de exporters OTLP hacia el Collector
 */
@Configuration
public class ObservationConfig {
}
