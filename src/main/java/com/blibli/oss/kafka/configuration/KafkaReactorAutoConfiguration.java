package com.blibli.oss.kafka.configuration;

import com.blibli.oss.kafka.producer.PlainKafkaProducer;
import com.blibli.oss.kafka.producer.ReactorKafkaProducer;
import com.blibli.oss.kafka.producer.impl.ReactorKafkaProducerImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ConditionalOnClass({Mono.class, Flux.class})
@Configuration
@AutoConfigureAfter({KafkaAutoConfiguration.class})
public class KafkaReactorAutoConfiguration {

  @Bean
  public ReactorKafkaProducer kafkaProducer(@Autowired PlainKafkaProducer plainKafkaProducer) {
    return new ReactorKafkaProducerImpl(plainKafkaProducer);
  }

}
