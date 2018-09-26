package com.blibli.oss.kafka.interceptor;

import com.blibli.oss.kafka.error.KafkaException;
import com.blibli.oss.kafka.interceptor.events.ConsumerEvent;
import com.blibli.oss.kafka.interceptor.events.ProducerEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.core.OrderComparator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author Eko Kurniawan Khannedy
 * @since 11/02/18
 */
@Slf4j
public class InterceptorUtil {

  public static void fireBeforeSend(List<KafkaProducerInterceptor> kafkaProducerInterceptors, ProducerEvent event) {
    for (KafkaProducerInterceptor interceptor : kafkaProducerInterceptors) {
      try {
        interceptor.beforeSend(event);
      } catch (Throwable throwable) {
        log.error("Error while invoking interceptor", throwable);
        if (throwable instanceof KafkaException) {
          throw (KafkaException) throwable;
        }
      }
    }
  }

  public static boolean fireBeforeConsume(List<KafkaConsumerInterceptor> kafkaConsumerInterceptors, ConsumerEvent event) {
    for (KafkaConsumerInterceptor interceptor : kafkaConsumerInterceptors) {
      try {
        if (interceptor.beforeConsume(event)) {
          return true;
        }
      } catch (Throwable throwable) {
        log.error("Error while invoke interceptor", throwable);
        if (throwable instanceof KafkaException) {
          throw (KafkaException) throwable;
        }
      }
    }
    return false;
  }

  public static void fireAfterSuccessConsume(List<KafkaConsumerInterceptor> kafkaConsumerInterceptors, ConsumerEvent event) {
    for (KafkaConsumerInterceptor interceptor : kafkaConsumerInterceptors) {
      try {
        interceptor.afterSuccessConsume(event);
      } catch (Throwable throwable) {
        log.error("Error while invoke interceptor", throwable);
        if (throwable instanceof KafkaException) {
          throw (KafkaException) throwable;
        }
      }
    }
  }

  public static void fireAfterErrorConsume(List<KafkaConsumerInterceptor> kafkaConsumerInterceptors, ConsumerEvent event, Throwable throwable) {
    for (KafkaConsumerInterceptor interceptor : kafkaConsumerInterceptors) {
      try {
        interceptor.afterFailedConsume(event, throwable);
      } catch (Throwable e) {
        log.error("Error while invoke interceptor", e);
        if (throwable instanceof KafkaException) {
          throw (KafkaException) throwable;
        }
      }
    }
  }

  /**
   * Get kafka producer interceptors
   *
   * @param applicationContext application context
   * @return ordered producer interceptors
   */
  public static List<KafkaProducerInterceptor> getKafkaProducerInterceptors(ApplicationContext applicationContext) {
    List<KafkaProducerInterceptor> interceptors = Collections.emptyList();

    Map<String, KafkaProducerInterceptor> beans = applicationContext.getBeansOfType(KafkaProducerInterceptor.class);
    if (beans != null && !beans.isEmpty()) {
      interceptors = new ArrayList<>(beans.values());
    }

    OrderComparator.sort(interceptors);
    return interceptors;
  }

  /**
   * Get kafka consumer interceptors
   *
   * @param applicationContext application context
   * @return ordered consumer interceptors
   */
  public static List<KafkaConsumerInterceptor> getKafkaConsumerInterceptors(ApplicationContext applicationContext) {
    List<KafkaConsumerInterceptor> interceptors = Collections.emptyList();

    Map<String, KafkaConsumerInterceptor> beans = applicationContext.getBeansOfType(KafkaConsumerInterceptor.class);
    if (beans != null && !beans.isEmpty()) {
      interceptors = new ArrayList<>(beans.values());
    }

    OrderComparator.sort(interceptors);
    return interceptors;
  }

}
