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

package com.blibli.oss.kafka.producer.impl;

import com.blibli.oss.kafka.interceptor.InterceptorUtil;
import com.blibli.oss.kafka.interceptor.KafkaProducerInterceptor;
import com.blibli.oss.kafka.interceptor.events.ProducerEvent;
import com.blibli.oss.kafka.producer.KafkaProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import rx.Single;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * @author Eko Kurniawan Khannedy
 */
@Slf4j
public class KafkaProducerImpl implements KafkaProducer, ApplicationContextAware, InitializingBean {

  private ObjectMapper objectMapper;

  private KafkaTemplate<String, String> kafkaTemplate;

  private ApplicationContext applicationContext;

  private List<KafkaProducerInterceptor> kafkaProducerInterceptors;

  public KafkaProducerImpl(ObjectMapper objectMapper, KafkaTemplate<String, String> kafkaTemplate) {
    this.objectMapper = objectMapper;
    this.kafkaTemplate = kafkaTemplate;
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    kafkaProducerInterceptors = InterceptorUtil.getKafkaProducerInterceptors(applicationContext);
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    this.applicationContext = applicationContext;
  }

  @Override
  public Single<SendResult<String, String>> send(String topic, String key, Object message, Integer partition, Long timestamp) {
    try {
      ProducerEvent event = ProducerEvent.builder()
          .key(key)
          .topic(topic)
          .value(message)
          .partition(partition)
          .timestamp(timestamp)
          .build();

      InterceptorUtil.fireBeforeSend(kafkaProducerInterceptors, event);

      String json = objectMapper.writeValueAsString(event.getValue());
      Future<SendResult<String, String>> result = kafkaTemplate.send(
          new ProducerRecord<>(event.getTopic(), event.getPartition(), event.getTimestamp(), event.getKey(), json)
      );

      return Single.from(result);
    } catch (JsonProcessingException e) {
      return Single.error(e);
    }
  }
}
