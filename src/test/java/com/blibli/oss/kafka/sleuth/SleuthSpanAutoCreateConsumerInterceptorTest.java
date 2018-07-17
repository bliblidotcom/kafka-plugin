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

import com.blibli.oss.kafka.interceptor.events.ConsumerEvent;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.core.Ordered;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Eko Kurniawan Khannedy
 */
public class SleuthSpanAutoCreateConsumerInterceptorTest {

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock
  private Tracer tracer;

  @InjectMocks
  private SleuthSpanAutoCreateConsumerInterceptor interceptor;

  private ConsumerEvent event = ConsumerEvent.builder()
    .topic("topic")
    .build();

  @Test
  public void testOrder() {
    assertEquals(Ordered.HIGHEST_PRECEDENCE + 1, interceptor.getOrder());
  }

  @Test
  public void testBeforeConsume() {
    assertFalse(interceptor.beforeConsume(event));

    verify(tracer).getCurrentSpan();
    verify(tracer).createSpan("kafka:consumer:topic");
  }

  @After
  public void tearDown() throws Exception {
    verifyNoMoreInteractions(tracer);
  }
}