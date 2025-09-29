package com.egm.stellio.shared.config

import TransactionalEither
import TransactionalEitherInterceptor
import org.springframework.aop.framework.autoproxy.DefaultAdvisorAutoProxyCreator
import org.springframework.aop.support.DefaultPointcutAdvisor
import org.springframework.aop.support.annotation.AnnotationMatchingPointcut
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.transaction.PlatformTransactionManager

@Configuration
class AOPConfig {

    @Bean
    fun proxyCreator() = DefaultAdvisorAutoProxyCreator()

    @Bean
    fun transactionalEitherInterceptor(transactionManager: PlatformTransactionManager) =
        TransactionalEitherInterceptor(transactionManager)

    @Bean
    fun transactionalEitherAdvisor(
        transactionalEitherInterceptor: TransactionalEitherInterceptor
    ): DefaultPointcutAdvisor {
        val pointcut = AnnotationMatchingPointcut(null, TransactionalEither::class.java)
        return DefaultPointcutAdvisor(pointcut, transactionalEitherInterceptor)
    }
}
