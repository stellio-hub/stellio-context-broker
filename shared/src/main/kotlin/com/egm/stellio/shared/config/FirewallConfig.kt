package com.egm.stellio.shared.config

import org.springframework.beans.BeansException
import org.springframework.beans.factory.config.BeanPostProcessor
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.security.web.server.WebFilterChainProxy
import org.springframework.security.web.server.firewall.StrictServerWebExchangeFirewall

@Configuration
class FirewallConfig {

    @Bean
    fun beanPostProcessor(): BeanPostProcessor {
        return object : BeanPostProcessor {
            @Throws(BeansException::class)
            override fun postProcessBeforeInitialization(bean: Any, beanName: String): Any {
                if (bean is WebFilterChainProxy) {
                    val firewall = StrictServerWebExchangeFirewall()
                    firewall.setAllowUrlEncodedSlash(true)
                    firewall.setAllowUrlEncodedDoubleSlash(true)
                    bean.setFirewall(firewall)
                }
                return bean
            }
        }
    }
}
