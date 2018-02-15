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

package com.blibli.oss.kafka.sleuth;

import com.blibli.oss.kafka.interceptor.KafkaConsumerInterceptor;
import com.blibli.oss.kafka.interceptor.KafkaProducerInterceptor;
import com.blibli.oss.kafka.interceptor.events.ConsumerEvent;
import com.blibli.oss.kafka.interceptor.events.ProducerEvent;
import com.blibli.oss.kafka.properties.KafkaProperties;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.core.Ordered;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * @author Eko Kurniawan Khannedy
 */
@Slf4j
public class SleuthSpanInterceptor implements KafkaConsumerInterceptor, KafkaProducerInterceptor {

  private KafkaProperties.ModelProperties modelProperties;

  private ObjectMapper objectMapper;

  private Tracer tracer;

  public SleuthSpanInterceptor(KafkaProperties.ModelProperties modelProperties, ObjectMapper objectMapper, Tracer tracer) {
    this.modelProperties = modelProperties;
    this.objectMapper = objectMapper;
    this.tracer = tracer;
  }

  @Override
  public void beforeSend(ProducerEvent event) {
    PropertyDescriptor descriptor = BeanUtils.getPropertyDescriptor(event.getValue().getClass(), modelProperties.getTrace());
    if (descriptor != null) {
      Method method = descriptor.getWriteMethod();
      if (method != null) {
        try {
          Map<String, String> span = SleuthHelper.toMap(tracer.getCurrentSpan());
          method.invoke(event.getValue(), span);
          log.debug("Inject trace span {} to message", span);
        } catch (Throwable e) {
          log.error("Error while write span information", e);
        }
      }
    }
  }

  @Override
  public boolean beforeConsume(ConsumerEvent event) {
    try {
      JsonNode node = objectMapper.readTree(event.getValue());
      JsonNode spanNode = node.get(modelProperties.getTrace());
      if (spanNode != null) {
        JsonParser jsonParser = objectMapper.treeAsTokens(spanNode);
        Map<String, String> span = objectMapper.readValue(jsonParser, new TypeReference<Map<String, String>>() {
        });
        SleuthHelper.continueSpan(tracer, span);
        log.debug("Continue trace span {}", span);
      }
    } catch (Throwable throwable) {
      log.error("Failed continue span", throwable);
    }

    return false;
  }

  @Override
  public int getOrder() {
    return Ordered.HIGHEST_PRECEDENCE;
  }
}
