package com.egm.stellio.apigateway.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.security.web.server.firewall.ServerWebExchangeFirewall
import org.springframework.security.web.server.firewall.StrictServerWebExchangeFirewall

@Configuration
class FirewallConfig {

    @Bean
    fun httpFirewall(): ServerWebExchangeFirewall =
        StrictServerWebExchangeFirewall().apply {
            setAllowUrlEncodedDoubleSlash(true)
            setAllowUrlEncodedSlash(true)
            setAllowUrlEncodedPercent(true)
        }
}
