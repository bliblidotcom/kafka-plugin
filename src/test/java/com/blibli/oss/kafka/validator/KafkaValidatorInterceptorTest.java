package com.blibli.oss.kafka.validator;

import com.blibli.oss.kafka.error.InvalidSchemaException;
import com.blibli.oss.kafka.producer.KafkaProducer;
import lombok.Builder;
import lombok.Data;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hibernate.validator.constraints.NotBlank;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import static org.mockito.Mockito.*;

/**
 * @author Eko Kurniawan Khannedy
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = KafkaValidatorInterceptorTest.Application.class)
@TestPropertySource(
    properties = {
        "kafka.plugin.feature.schema-validator=true"
    }
)
public class KafkaValidatorInterceptorTest {

  @MockBean
  private KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  private KafkaProducer kafkaProducer;

  @Before
  public void setUp() throws Exception {
    when(kafkaTemplate.send(any(ProducerRecord.class)))
        .thenReturn(new AsyncResult(new SendResult<>(null, null)));
  }

  @Test(expected = InvalidSchemaException.class)
  public void testFailed() {
    SampleRequest request = SampleRequest.builder().build();

    kafkaProducer.send("test", request).toBlocking().value();
  }

  @Test
  public void testSuccess() {
    SampleRequest request = SampleRequest.builder()
        .data("Not Blank")
        .build();

    kafkaProducer.send("test", request).toBlocking().value();
  }

  @SpringBootApplication
  public static class Application {

  }

  @Data
  @Builder
  private static class SampleRequest {

    @NotBlank
    private String data;

  }

}