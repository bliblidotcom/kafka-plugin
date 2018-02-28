package com.blibli.oss.kafka.error;

import javax.validation.ConstraintViolation;
import java.util.Set;

/**
 * @author Eko Kurniawan Khannedy
 */
public class InvalidSchemaException extends KafkaException {

  private Set<ConstraintViolation<Object>> constraintViolations;

  public InvalidSchemaException(Set<ConstraintViolation<Object>> constraintViolations) {
    this((String) null, constraintViolations);
  }

  public InvalidSchemaException(String message, Set<ConstraintViolation<Object>> constraintViolations) {
    this((Throwable) null, constraintViolations);
  }

  public InvalidSchemaException(Throwable cause, Set<ConstraintViolation<Object>> constraintViolations) {
    this(null, cause, constraintViolations);
  }

  public InvalidSchemaException(String message, Throwable cause, Set<ConstraintViolation<Object>> constraintViolations) {
    super(message, cause);
    this.constraintViolations = constraintViolations;
  }

  public Set<ConstraintViolation<Object>> getConstraintViolations() {
    return constraintViolations;
  }
}
