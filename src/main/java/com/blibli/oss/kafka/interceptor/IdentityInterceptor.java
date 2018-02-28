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

package com.blibli.oss.kafka.interceptor;

import com.blibli.oss.kafka.interceptor.events.ProducerEvent;
import com.blibli.oss.kafka.properties.KafkaProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.UUID;

/**
 * @author Eko Kurniawan Khannedy
 */
@Slf4j
public class IdentityInterceptor implements KafkaProducerInterceptor {

  private KafkaProperties kafkaProperties;

  public IdentityInterceptor(KafkaProperties kafkaProperties) {
    this.kafkaProperties = kafkaProperties;
  }

  @Override
  public void beforeSend(ProducerEvent event) {
    PropertyDescriptor descriptor = BeanUtils.getPropertyDescriptor(event.getValue().getClass(),
        kafkaProperties.getModel().getIdentity());
    if (descriptor != null) {
      writeIdentityId(event.getValue(), descriptor);
    }
  }

  private void writeIdentityId(Object message, PropertyDescriptor descriptor) {
    Method method = descriptor.getWriteMethod();
    if (method != null) {
      try {
        String eventId = UUID.randomUUID().toString();
        method.invoke(message, eventId);
        log.debug("Inject event id {} to message", eventId);
      } catch (IllegalAccessException | InvocationTargetException e) {
        if (kafkaProperties.getLog().isWhenFailedSetEventId()) {
          log.warn("Error while write identity id", e);
        }
      }
    }
  }
}
