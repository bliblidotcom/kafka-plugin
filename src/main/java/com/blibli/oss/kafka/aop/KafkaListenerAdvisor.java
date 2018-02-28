package com.blibli.oss.kafka.aop;

import org.aopalliance.aop.Advice;
import org.springframework.aop.Pointcut;
import org.springframework.aop.support.AbstractPointcutAdvisor;

/**
 * @author Eko Kurniawan Khannedy
 */
public class KafkaListenerAdvisor extends AbstractPointcutAdvisor {

  private KafkaListenerPointcut pointcut;

  private KafkaListenerInterceptor interceptor;

  public KafkaListenerAdvisor(KafkaListenerPointcut pointcut, KafkaListenerInterceptor interceptor) {
    this.pointcut = pointcut;
    this.interceptor = interceptor;
  }

  @Override
  public Pointcut getPointcut() {
    return pointcut;
  }

  @Override
  public Advice getAdvice() {
    return interceptor;
  }
}
