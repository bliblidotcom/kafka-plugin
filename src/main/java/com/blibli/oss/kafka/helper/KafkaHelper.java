package com.blibli.oss.kafka.helper;

import com.blibli.oss.kafka.interceptor.events.ConsumerEvent;
import com.blibli.oss.kafka.properties.KafkaProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Optional;

/**
 * @author Eko Kurniawan Khannedy
 */
@Slf4j
public class KafkaHelper {

  public static ConsumerEvent toConsumerEvent(ConsumerRecord<String, String> record, String eventId) {
    return ConsumerEvent.builder()
      .eventId(eventId)
      .key(record.key())
      .partition(record.partition())
      .timestamp(record.timestamp())
      .topic(record.topic())
      .value(record.value())
      .build();
  }

  public static String getEventId(String message, ObjectMapper objectMapper, KafkaProperties.ModelProperties modelProperties) {
    try {
      JsonNode jsonNode = objectMapper.readTree(message);
      return Optional.ofNullable(jsonNode.get(modelProperties.getIdentity()))
        .map(JsonNode::asText)
        .orElse(null);
    } catch (Throwable throwable) {
      log.warn("Error while get event id", throwable);
      return null;
    }
  }

}
