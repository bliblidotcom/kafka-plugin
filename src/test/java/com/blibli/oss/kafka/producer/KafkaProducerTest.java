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

package com.blibli.oss.kafka.producer;

import com.blibli.oss.kafka.producer.impl.KafkaProducerImpl;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Data;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.Map;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.*;

/**
 * @author Eko Kurniawan Khannedy
 */
public class KafkaProducerTest {

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock
  private ObjectMapper objectMapper;

  @Mock
  private KafkaTemplate<String, String> kafkaTemplate;

  @Mock
  private Tracer tracer;

  @InjectMocks
  private KafkaProducerImpl merchantProducer;

  @Mock
  private SendResult<String, String> sendResult;

  private Request request;

  @Before
  public void setUp() throws Exception {
    mockObjectMapperSuccessWriteValueAsJson();
    when(tracer.getCurrentSpan()).thenReturn(null);

    request = Request.builder().id("ID").build();
  }

  private void mockObjectMapperSuccessWriteValueAsJson() throws JsonProcessingException {
    when(objectMapper.writeValueAsString(anyObject()))
        .thenReturn("{}");
  }

  @Test
  public void testSendSuccess() throws Exception {
    mockKafkaTemplateWithSuccessResult();

    SendResult<String, String> value = merchantProducer.send("TOPIC_NAME", request)
        .toBlocking().value();

    assertSame(sendResult, value);
  }

  private void mockKafkaTemplateWithSuccessResult() {
    SettableListenableFuture<SendResult<String, String>> future = new SettableListenableFuture<>();
    future.set(sendResult);

    when(kafkaTemplate.send(anyString(), anyString(), anyString()))
        .thenReturn(future);
  }

  @Test(expected = RuntimeException.class)
  public void testSendError() throws Exception {
    mockKafkaTemplateWithExceptionResult();

    merchantProducer.send("TOPIC_NAME", request)
        .toBlocking().value();
  }

  private void mockKafkaTemplateWithExceptionResult() {
    SettableListenableFuture<SendResult<String, String>> future = new SettableListenableFuture<>();
    future.setException(new NullPointerException());

    when(kafkaTemplate.send(anyString(), anyString(), anyString()))
        .thenReturn(future);
  }

  @Test(expected = RuntimeException.class)
  public void testSendErrorBecuaseParsingJson() throws Exception {
    mockObjectMapperErrorParsing();

    merchantProducer.send("TOPIC_NAME", request)
        .toBlocking().value();
  }

  private void mockObjectMapperErrorParsing() throws JsonProcessingException {
    when(objectMapper.writeValueAsString(anyObject()))
        .thenThrow(new JsonParseException(null, "Parsing Error"));
  }

  @Data
  @Builder
  public static class Request {

    private String id;

    private Map<String, String> span;

    private String routingId;
  }

}