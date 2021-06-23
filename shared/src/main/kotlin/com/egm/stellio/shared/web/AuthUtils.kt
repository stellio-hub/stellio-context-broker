package com.egm.stellio.shared.web

import org.springframework.security.core.context.ReactiveSecurityContextHolder
import org.springframework.security.core.context.SecurityContextImpl
import org.springframework.security.oauth2.jwt.Jwt
import reactor.core.publisher.Mono
import java.net.URI
import java.util.UUID

fun extractSubjectOrEmpty(): Mono<String> {
    return ReactiveSecurityContextHolder.getContext()
        .switchIfEmpty(Mono.just(SecurityContextImpl()))
        .map { context -> context.authentication?.principal?.let { (it as Jwt).subject } ?: "" }
}

fun URI.extractSubjectUuid(): UUID =
    UUID.fromString(this.toString().substringAfterLast(":"))

fun String.toUUID(): UUID =
    UUID.fromString(this)

fun getSubjectTypeFromSubjectId(subjectId: URI): SubjectType {
    val type = subjectId.toString().split(":")[2]
    return SubjectType.valueOf(type.toUpperCase())
}

enum class SubjectType {
    USER,
    GROUP,
    CLIENT
}
