package com.egm.stellio.shared.util

import arrow.core.Option
import arrow.core.toOption
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.util.GlobalRole.STELLIO_ADMIN
import com.egm.stellio.shared.util.GlobalRole.STELLIO_CREATOR
import com.egm.stellio.shared.util.JsonLdUtils.EGM_BASE_CONTEXT_URL
import org.springframework.security.core.context.ReactiveSecurityContextHolder
import org.springframework.security.core.context.SecurityContextImpl
import org.springframework.security.oauth2.jwt.Jwt
import reactor.core.publisher.Mono
import java.net.URI
import java.util.UUID

val ADMIN_ROLES: Set<GlobalRole> = setOf(STELLIO_ADMIN)
val CREATION_ROLES: Set<GlobalRole> = setOf(STELLIO_CREATOR).plus(ADMIN_ROLES)

object AuthContextModel {
    val NGSILD_EGM_AUTHORIZATION_CONTEXT = "$EGM_BASE_CONTEXT_URL/authorization/jsonld-contexts/authorization.jsonld"
    private const val AUTHORIZATION_ONTOLOGY = "https://ontology.eglobalmark.com/authorization#"

    const val USER_PREFIX = "urn:ngsi-ld:User:"

    const val USER_TYPE: ExpandedTerm = AUTHORIZATION_ONTOLOGY + "User"
    const val GROUP_TYPE: ExpandedTerm = AUTHORIZATION_ONTOLOGY + "Group"
    const val CLIENT_TYPE: ExpandedTerm = AUTHORIZATION_ONTOLOGY + "Client"
    val IAM_TYPES = setOf(USER_TYPE, GROUP_TYPE, CLIENT_TYPE)

    const val AUTH_TERM_SID = "serviceAccountId"
    const val AUTH_PROP_SID: ExpandedTerm = AUTHORIZATION_ONTOLOGY + AUTH_TERM_SID
    const val AUTH_TERM_ROLES = "roles"
    const val AUTH_PROP_ROLES: ExpandedTerm = AUTHORIZATION_ONTOLOGY + AUTH_TERM_ROLES
    const val AUTH_TERM_USERNAME = "username"
    const val AUTH_PROP_USERNAME: ExpandedTerm = AUTHORIZATION_ONTOLOGY + AUTH_TERM_USERNAME
    const val AUTH_TERM_SAP = "specificAccessPolicy"
    const val AUTH_PROP_SAP = AUTHORIZATION_ONTOLOGY + AUTH_TERM_SAP

    const val AUTH_TERM_IS_MEMBER_OF = "isMemberOf"
    const val AUTH_REL_IS_MEMBER_OF: ExpandedTerm = AUTHORIZATION_ONTOLOGY + AUTH_TERM_IS_MEMBER_OF
    const val AUTH_REL_CAN_READ: ExpandedTerm = AUTHORIZATION_ONTOLOGY + "rCanRead"
    const val AUTH_REL_CAN_WRITE: ExpandedTerm = AUTHORIZATION_ONTOLOGY + "rCanWrite"
    const val AUTH_REL_CAN_ADMIN: ExpandedTerm = AUTHORIZATION_ONTOLOGY + "rCanAdmin"
    val ALL_IAM_RIGHTS = setOf(AUTH_REL_CAN_READ, AUTH_REL_CAN_WRITE, AUTH_REL_CAN_ADMIN)
    val ADMIN_RIGHTS: Set<String> = setOf(AUTH_REL_CAN_ADMIN)
    val WRITE_RIGHTS: Set<String> = setOf(AUTH_REL_CAN_WRITE).plus(ADMIN_RIGHTS)
    val READ_RIGHTS: Set<String> = setOf(AUTH_REL_CAN_READ).plus(WRITE_RIGHTS)

    enum class SpecificAccessPolicy {
        AUTH_READ,
        AUTH_WRITE
    }
}

fun extractSubjectOrEmpty(): Mono<String> {
    return ReactiveSecurityContextHolder.getContext()
        .switchIfEmpty(Mono.just(SecurityContextImpl()))
        .map { context -> context.authentication?.principal?.let { (it as Jwt).subject } ?: "" }
}

fun URI.extractSubjectUuid(): UUID =
    this.toString().extractSubjectUuid()

fun String.extractSubjectUuid(): UUID =
    UUID.fromString(this.substringAfterLast(":"))

fun String.toUUID(): UUID =
    UUID.fromString(this)

// specific to authz terms where we know the compacted term is what is after the last # character
fun ExpandedTerm.toCompactTerm(): String = this.substringAfterLast("#")

enum class SubjectType {
    USER,
    GROUP,
    CLIENT
}

enum class GlobalRole(val key: String) {
    STELLIO_CREATOR("stellio-creator"),
    STELLIO_ADMIN("stellio-admin");

    companion object {
        fun forKey(key: String): Option<GlobalRole> =
            values().find { it.key == key }.toOption()
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
