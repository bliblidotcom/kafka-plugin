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
import com.blibli.oss.kafka.interceptor.events.ConsumerEvent;
import com.blibli.oss.kafka.properties.KafkaProperties;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.cloud.sleuth.autoconfig.SleuthProperties;
import org.springframework.core.Ordered;

import java.util.Map;

/**
 * @author Eko Kurniawan Khannedy
 */
@Slf4j
public class SleuthSpanConsumerInterceptor implements KafkaConsumerInterceptor {

  private KafkaProperties.ModelProperties modelProperties;

  private ObjectMapper objectMapper;

  private Tracer tracer;

  private SleuthProperties sleuthProperties;

  public SleuthSpanConsumerInterceptor(KafkaProperties.ModelProperties modelProperties, ObjectMapper objectMapper,
                                       Tracer tracer, SleuthProperties sleuthProperties) {
    this.modelProperties = modelProperties;
    this.objectMapper = objectMapper;
    this.tracer = tracer;
    this.sleuthProperties = sleuthProperties;
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

        if (sleuthProperties.isSupportsJoin()) {
          SleuthHelper.joinSpan(tracer, span);
          log.debug("Join trace span {}", span);
        } else {
          SleuthHelper.continueSpan(tracer, span);
          log.debug("Continue trace span {}", span);
        }
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
