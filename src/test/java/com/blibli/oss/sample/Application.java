/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.blibli.oss.sample;

import com.blibli.oss.kafka.interceptor.KafkaConsumerDuplicateCheckInterceptor;
import com.blibli.oss.kafka.producer.KafkaProducer;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import rx.Observable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Eko Kurniawan Khannedy
 */
@SpringBootApplication
public class Application {

  public static void main(String[] args) {
    ConfigurableApplicationContext applicationContext = SpringApplication.run(Application.class);
    KafkaProducer kafkaProducer = applicationContext.getBean(KafkaProducer.class);

    Observable.interval(1, TimeUnit.SECONDS).forEach(aLong -> {
      Model model = Model.builder()
          .name("Test")
          .build();

      kafkaProducer.send("hello", model).toBlocking().value();
    });
  }

  @Slf4j
  @Component
  public static class RejectInterceptorConsumer implements KafkaConsumerDuplicateCheckInterceptor {

    private List<String> list = new ArrayList<>();

    @Override
    public boolean isAlreadyConsumed(String eventId) {
      log.info("Check is {} already consumed", eventId);
      return list.contains(eventId);
    }

    @Override
    public void markAsAlreadyConsumed(String eventId) {
      log.info("Mark {} as already consumed", eventId);
      list.add(eventId);
    }
  }

  @Slf4j
  @Component
  public static class Listener {

    @KafkaListener(topics = "hello")
    public void onMessage(ConsumerRecord<String, String> record) {
      log.info("Consume message {}", record.value());
    }

  }

  @Data
  @Builder
  public static class Model {

    private String eventId;

    private String name;

    private String routingId;

    private Map<String, String> span;

  }

}
