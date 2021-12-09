package com.egm.stellio.shared.util

import com.egm.stellio.shared.util.GlobalRole.STELLIO_ADMIN
import com.egm.stellio.shared.util.GlobalRole.STELLIO_CREATOR
import org.springframework.security.core.context.ReactiveSecurityContextHolder
import org.springframework.security.core.context.SecurityContextImpl
import org.springframework.security.oauth2.jwt.Jwt
import reactor.core.publisher.Mono
import java.net.URI
import java.util.UUID

val ADMIN_ROLES: Set<GlobalRole> = setOf(STELLIO_ADMIN)
val CREATION_ROLES: Set<GlobalRole> = setOf(STELLIO_CREATOR).plus(ADMIN_ROLES)

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

enum class GlobalRole(val key: String) {
    STELLIO_CREATOR("stellio-creator"),
    STELLIO_ADMIN("stellio-admin");

    companion object {
        fun forKey(key: String): GlobalRole =
            values().find { it.key == key } ?: throw IllegalArgumentException("Unrecognized key $key")
    }
}

enum class AccessRight(val attributeName: String) {
    R_CAN_READ("rCanRead"),
    R_CAN_WRITE("rCanWrite"),
    R_CAN_ADMIN("rCanAdmin");

    companion object {
        fun forAttributeName(attributeName: String): AccessRight =
            values().find {
                it.attributeName == attributeName
            } ?: throw IllegalArgumentException("Unrecognized attribute name $attributeName")
    }
}
