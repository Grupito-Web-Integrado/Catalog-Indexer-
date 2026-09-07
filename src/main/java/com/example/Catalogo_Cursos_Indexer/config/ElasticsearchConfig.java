package com.example.Catalogo_Cursos_Indexer.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticsearchConfig {

  @Value("${elasticsearch.host:localhost}")
  private String host;

  @Value("${elasticsearch.port:9200}")
  private int port;

  @Value("${elasticsearch.scheme:http}")
  private String scheme;

  @Value("${elasticsearch.username:}")
  private String username;

  @Value("${elasticsearch.password:}")
  private String password;

  @Bean
  public RestClient restClient() {

    org.elasticsearch.client.RestClientBuilder builder = RestClient.builder(
        new HttpHost(
            host,
            port,
            scheme));

    if (username != null &&
        !username.isBlank()) {

      CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

      credentialsProvider.setCredentials(
          AuthScope.ANY,
          new UsernamePasswordCredentials(
              username,
              password));

      builder.setHttpClientConfigCallback(
          httpClientBuilder -> httpClientBuilder
              .setDefaultCredentialsProvider(
                  credentialsProvider));
    }

    return builder.build();
  }

  @Bean
  public ElasticsearchTransport elasticsearchTransport(
      RestClient restClient,
      ObjectMapper objectMapper) {

    ObjectMapper elasticsearchObjectMapper = objectMapper.copy();

    elasticsearchObjectMapper.registerModule(
        new JavaTimeModule());

    elasticsearchObjectMapper.disable(
        SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    return new RestClientTransport(
        restClient,
        new JacksonJsonpMapper(
            elasticsearchObjectMapper));
  }

  @Bean
  public ElasticsearchClient elasticsearchClient(
      ElasticsearchTransport transport) {

    return new ElasticsearchClient(
        transport);
  }
}
