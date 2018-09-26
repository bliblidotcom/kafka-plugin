package com.blibli.oss.kafka.configuration;

import com.blibli.oss.kafka.properties.KafkaProperties;
import com.blibli.oss.kafka.validator.KafkaValidatorInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.kafka.annotation.EnableKafka;

import javax.validation.Validator;

/**
 * @author Eko Kurniawan Khannedy
 */
@Configuration
@EnableKafka
@EnableAspectJAutoProxy
@ConditionalOnClass({Validator.class})
@AutoConfigureAfter({KafkaPropertiesAutoConfiguration.class})
public class KafkaValidatorAutoConfiguration {

  @Bean
  public KafkaValidatorInterceptor kafkaValidatorInterceptor(@Autowired Validator validator,
                                                             @Autowired KafkaProperties kafkaProperties) {
    return new KafkaValidatorInterceptor(validator, kafkaProperties);
  }

}
