package com.egm.stellio.shared.util

import org.springframework.security.core.context.ReactiveSecurityContextHolder
import org.springframework.security.core.context.SecurityContextImpl
import org.springframework.security.oauth2.jwt.Jwt
import reactor.core.publisher.Mono
import java.net.URI
import java.util.UUID

const val ADMIN_ROLE_LABEL = "stellio-admin"
const val CREATION_ROLE_LABEL = "stellio-creator"
val ADMIN_ROLES: Set<String> = setOf(ADMIN_ROLE_LABEL)
val CREATION_ROLES: Set<String> = setOf(CREATION_ROLE_LABEL).plus(ADMIN_ROLES)

fun extractSubjectOrEmpty(): Mono<String> {
    return ReactiveSecurityContextHolder.getContext()
        .switchIfEmpty(Mono.just(SecurityContextImpl()))
        .map { context -> context.authentication?.principal?.let { (it as Jwt).subject } ?: "" }
}

fun URI.extractSubjectUuid(): UUID =
    UUID.fromString(this.toString().substringAfterLast(":"))

fun String.toUUID(): UUID =
    UUID.fromString(this)

enum class SubjectType {
    USER,
    GROUP,
    CLIENT
}
