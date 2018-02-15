package com.blibli.oss.kafka.interceptor;

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
public class InterceptorUtil {

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
