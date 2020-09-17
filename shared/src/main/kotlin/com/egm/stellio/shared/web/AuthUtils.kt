package com.egm.stellio.shared.web

import com.egm.stellio.shared.util.toUri
import org.springframework.security.core.context.ReactiveSecurityContextHolder
import org.springframework.security.core.context.SecurityContextImpl
import org.springframework.security.oauth2.jwt.Jwt
import reactor.core.publisher.Mono
import java.net.URI

fun extractSubjectOrEmpty(): Mono<URI> {
    return ReactiveSecurityContextHolder.getContext()
        .switchIfEmpty(Mono.just(SecurityContextImpl()))
        .map { context -> context.authentication?.principal?.let { ((it as Jwt).subject).toUri() } ?: "".toUri() }
}
