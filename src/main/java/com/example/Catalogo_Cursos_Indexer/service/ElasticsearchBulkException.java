package com.example.Catalogo_Cursos_Indexer.service;

public class ElasticsearchBulkException extends RuntimeException {
  public ElasticsearchBulkException(String message, Throwable cause) {
    super(message, cause);
  }
}
