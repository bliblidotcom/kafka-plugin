package com.blibli.oss.kafka.aop;

import org.springframework.aop.support.StaticMethodMatcherPointcut;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.kafka.annotation.KafkaListener;

import java.lang.reflect.Method;

/**
 * @author Eko Kurniawan Khannedy
 */
public class KafkaListenerPointcut extends StaticMethodMatcherPointcut {

  @Override
  public boolean matches(Method method, Class<?> targetClass) {
    return AnnotationUtils.findAnnotation(method, KafkaListener.class) != null;
  }
}
