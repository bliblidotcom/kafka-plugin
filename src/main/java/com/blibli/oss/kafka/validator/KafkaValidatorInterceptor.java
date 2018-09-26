package com.blibli.oss.kafka.validator;

import com.blibli.oss.kafka.error.InvalidSchemaException;
import com.blibli.oss.kafka.interceptor.KafkaProducerInterceptor;
import com.blibli.oss.kafka.interceptor.events.ProducerEvent;
import com.blibli.oss.kafka.properties.KafkaProperties;

import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import java.util.Set;

/**
 * @author Eko Kurniawan Khannedy
 */
public class KafkaValidatorInterceptor implements KafkaProducerInterceptor {

  private Validator validator;

  private KafkaProperties kafkaProperties;

  public KafkaValidatorInterceptor(Validator validator, KafkaProperties kafkaProperties) {
    this.validator = validator;
    this.kafkaProperties = kafkaProperties;
  }

  @Override
  public void beforeSend(ProducerEvent event) {
    if (kafkaProperties.getFeature().isSchemaValidator()) {
      Set<ConstraintViolation<Object>> constraintViolations = validator.validate(event.getValue());
      if (!constraintViolations.isEmpty()) {
        throw new InvalidSchemaException(constraintViolations);
      }
    }
  }

  @Override
  public int getOrder() {
    return Integer.MAX_VALUE;
  }
}
