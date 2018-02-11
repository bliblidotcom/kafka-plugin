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

package com.blibli.oss.kafka.aspect;

import com.blibli.oss.kafka.interceptor.InterceptorUtil;
import com.blibli.oss.kafka.interceptor.KafkaConsumerInterceptor;
import com.blibli.oss.kafka.interceptor.events.ConsumerEvent;
import com.blibli.oss.kafka.properties.KafkaProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.List;

/**
 * @author Eko Kurniawan Khannedy
 */
@Slf4j
@Aspect
public class KafkaListenerAspect implements ApplicationContextAware, InitializingBean {

  private ApplicationContext applicationContext;

  private ObjectMapper objectMapper;

  private KafkaProperties.ModelProperties modelProperties;

  private List<KafkaConsumerInterceptor> kafkaConsumerInterceptors;

  public KafkaListenerAspect(ObjectMapper objectMapper, KafkaProperties.ModelProperties modelProperties) {
    this.objectMapper = objectMapper;
    this.modelProperties = modelProperties;
  }

  @Override
  public void afterPropertiesSet() {
    kafkaConsumerInterceptors = InterceptorUtil.getKafkaConsumerInterceptors(applicationContext);
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    this.applicationContext = applicationContext;
  }

  @Around(value = "@annotation(org.springframework.kafka.annotation.KafkaListener)")
  public Object around(ProceedingJoinPoint joinPoint) throws Throwable {
    if (joinPoint.getArgs().length == 0) {
      return joinPoint.proceed(joinPoint.getArgs());
    } else {
      return aroundWithInterceptor(joinPoint);
    }
  }

  private Object aroundWithInterceptor(ProceedingJoinPoint joinPoint) throws Throwable {
    ConsumerRecord<String, String> record = getConsumerRecord(joinPoint.getArgs());
    if (record != null) {
      ConsumerEvent event = toConsumerEvent(record);
      if (event != null) {
        if (beforeConsume(event)) {
          return null; // cancel process
        } else {
          try {
            Object result = joinPoint.proceed(joinPoint.getArgs());
            afterSuccessConsume(event);
            return result;
          } catch (Throwable throwable) {
            afterErrorConsume(event, throwable);
            throw throwable;
          }
        }
      }
    }

    return joinPoint.proceed(joinPoint.getArgs());
  }

  private ConsumerRecord<String, String> getConsumerRecord(Object[] objects) {
    if (objects == null) {
      return null;
    }

    for (Object object : objects) {
      if (object instanceof ConsumerRecord) {
        return (ConsumerRecord<String, String>) object;
      }
    }

    return null;
  }

  private ConsumerEvent toConsumerEvent(ConsumerRecord<String, String> record) {
    String evenId = getEventId(record.value());
    return ConsumerEvent.builder()
        .eventId(evenId)
        .key(record.key())
        .partition(record.partition())
        .timestamp(record.timestamp())
        .topic(record.topic())
        .value(record.value())
        .build();
  }

  private String getEventId(String message) {
    try {
      JsonNode jsonNode = objectMapper.readTree(message);
      return jsonNode.get(modelProperties.getIdentity()).asText();
    } catch (Throwable throwable) {
      log.error("Error while get event id", throwable);
      return null;
    }
  }

  private boolean beforeConsume(ConsumerEvent event) {
    for (KafkaConsumerInterceptor interceptor : kafkaConsumerInterceptors) {
      try {
        if (interceptor.beforeConsume(event)) {
          return true;
        }
      } catch (Throwable throwable) {
        log.error("Error while invoke interceptor", throwable);
      }
    }
    return false;
  }

  private void afterSuccessConsume(ConsumerEvent event) {
    for (KafkaConsumerInterceptor interceptor : kafkaConsumerInterceptors) {
      try {
        interceptor.afterSuccessConsume(event);
      } catch (Throwable throwable) {
        log.error("Error while invoke interceptor", throwable);
      }
    }
  }

  private void afterErrorConsume(ConsumerEvent event, Throwable throwable) {
    for (KafkaConsumerInterceptor interceptor : kafkaConsumerInterceptors) {
      try {
        interceptor.afterFailedConsume(event, throwable);
      } catch (Throwable e) {
        log.error("Error while invoke interceptor", e);
      }
    }
  }
}