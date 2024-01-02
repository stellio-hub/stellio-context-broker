package com.egm.stellio.search.config

import com.egm.stellio.shared.config.ApplicationProperties
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoders

@TestConfiguration
class WebSecurityTestConfig(
    private val applicationProperties: ApplicationProperties
) {

    @Bean
    fun jwtDecoder(): ReactiveJwtDecoder =
        ReactiveJwtDecoders.fromOidcIssuerLocation(applicationProperties.tenants[0].issuer)
}
