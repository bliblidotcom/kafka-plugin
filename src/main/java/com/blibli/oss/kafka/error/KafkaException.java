package com.blibli.oss.kafka.error;

/**
 * @author Eko Kurniawan Khannedy
 */
public class KafkaException extends RuntimeException {

  public KafkaException() {
  }

  public KafkaException(String message) {
    super(message);
  }

  public KafkaException(String message, Throwable cause) {
    super(message, cause);
  }

  public KafkaException(Throwable cause) {
    super(cause);
  }
}
