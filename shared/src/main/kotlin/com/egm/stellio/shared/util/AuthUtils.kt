package com.egm.stellio.shared.util

import arrow.core.*
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_ADMIN
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_READ
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_WRITE
import com.egm.stellio.shared.util.GlobalRole.STELLIO_ADMIN
import com.egm.stellio.shared.util.GlobalRole.STELLIO_CREATOR
import com.egm.stellio.shared.util.JsonLdUtils.EGM_BASE_CONTEXT_URL
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.security.core.context.ReactiveSecurityContextHolder
import org.springframework.security.core.context.SecurityContextImpl
import org.springframework.security.oauth2.jwt.Jwt
import reactor.core.publisher.Mono
import java.net.URI

val ADMIN_ROLES: Set<GlobalRole> = setOf(STELLIO_ADMIN)
val CREATION_ROLES: Set<GlobalRole> = setOf(STELLIO_CREATOR).plus(ADMIN_ROLES)

object AuthContextModel {
    const val NGSILD_AUTHORIZATION_CONTEXT = "$EGM_BASE_CONTEXT_URL/authorization/jsonld-contexts/authorization.jsonld"
    const val AUTHORIZATION_COMPOUND_CONTEXT =
        "$EGM_BASE_CONTEXT_URL/authorization/jsonld-contexts/authorization-compound.jsonld"
    val DEFAULT_AUTHZ_CONTEXT = listOf(NGSILD_AUTHORIZATION_CONTEXT, NGSILD_CORE_CONTEXT)

    private const val AUTHORIZATION_ONTOLOGY = "https://ontology.eglobalmark.com/authorization#"

    const val USER_COMPACT_TYPE = "User"
    const val USER_TYPE: ExpandedTerm = AUTHORIZATION_ONTOLOGY + USER_COMPACT_TYPE
    const val GROUP_COMPACT_TYPE = "Group"
    const val GROUP_TYPE: ExpandedTerm = AUTHORIZATION_ONTOLOGY + GROUP_COMPACT_TYPE
    const val CLIENT_COMPACT_TYPE = "Client"
    const val CLIENT_TYPE: ExpandedTerm = AUTHORIZATION_ONTOLOGY + CLIENT_COMPACT_TYPE
    val IAM_TYPES = setOf(USER_TYPE, GROUP_TYPE, CLIENT_TYPE)
    val IAM_COMPACTED_TYPES = setOf(USER_COMPACT_TYPE, GROUP_COMPACT_TYPE, CLIENT_COMPACT_TYPE)

    const val USER_ENTITY_PREFIX = "urn:ngsi-ld:User:"
    const val CLIENT_ENTITY_PREFIX = "urn:ngsi-ld:Client:"
    const val GROUP_ENTITY_PREFIX = "urn:ngsi-ld:Group:"

    const val AUTH_TERM_SUB = "sub"
    const val AUTH_TERM_CLIENT_ID = "clientId"
    const val AUTH_PROP_CLIENT_ID: ExpandedTerm = AUTHORIZATION_ONTOLOGY + AUTH_TERM_CLIENT_ID
    const val AUTH_TERM_NAME = "name"
    const val AUTH_TERM_SID = "serviceAccountId"
    const val AUTH_TERM_SUBJECT_INFO = "subjectInfo"
    const val AUTH_PROP_SUBJECT_INFO = AUTHORIZATION_ONTOLOGY + AUTH_TERM_SUBJECT_INFO
    const val AUTH_PROP_SID: ExpandedTerm = AUTHORIZATION_ONTOLOGY + AUTH_TERM_SID
    const val AUTH_TERM_ROLES = "roles"
    const val AUTH_PROP_ROLES: ExpandedTerm = AUTHORIZATION_ONTOLOGY + AUTH_TERM_ROLES
    const val AUTH_TERM_KIND = "kind"
    const val AUTH_PROP_KIND: ExpandedTerm = AUTHORIZATION_ONTOLOGY + AUTH_TERM_KIND
    const val AUTH_TERM_USERNAME = "username"
    const val AUTH_PROP_USERNAME: ExpandedTerm = AUTHORIZATION_ONTOLOGY + AUTH_TERM_USERNAME
    const val AUTH_TERM_GIVEN_NAME = "givenName"
    const val AUTH_TERM_FAMILY_NAME = "familyName"
    const val AUTH_TERM_SAP = "specificAccessPolicy"
    const val AUTH_PROP_SAP = AUTHORIZATION_ONTOLOGY + AUTH_TERM_SAP
    const val AUTH_TERM_RIGHT = "right"
    const val AUTH_PROP_RIGHT: ExpandedTerm = AUTHORIZATION_ONTOLOGY + AUTH_TERM_RIGHT
    val AUTH_SUBJECT_INFO_MEMBERS: Set<String> =
        setOf(AUTH_TERM_USERNAME, AUTH_TERM_GIVEN_NAME, AUTH_TERM_FAMILY_NAME, AUTH_TERM_CLIENT_ID, AUTH_TERM_NAME)

    const val AUTH_TERM_IS_MEMBER_OF = "isMemberOf"
    const val AUTH_REL_IS_MEMBER_OF: ExpandedTerm = AUTHORIZATION_ONTOLOGY + AUTH_TERM_IS_MEMBER_OF
    const val AUTH_TERM_CAN_READ = "rCanRead"
    const val AUTH_REL_CAN_READ: ExpandedTerm = AUTHORIZATION_ONTOLOGY + AUTH_TERM_CAN_READ
    const val AUTH_TERM_CAN_WRITE = "rCanWrite"
    const val AUTH_REL_CAN_WRITE: ExpandedTerm = AUTHORIZATION_ONTOLOGY + AUTH_TERM_CAN_WRITE
    const val AUTH_TERM_CAN_ADMIN = "rCanAdmin"
    const val AUTH_REL_CAN_ADMIN: ExpandedTerm = AUTHORIZATION_ONTOLOGY + AUTH_TERM_CAN_ADMIN
    val ALL_IAM_RIGHTS_TERMS = setOf(AUTH_TERM_CAN_READ, AUTH_TERM_CAN_WRITE, AUTH_TERM_CAN_ADMIN)
    val ALL_IAM_RIGHTS = setOf(AUTH_REL_CAN_READ, AUTH_REL_CAN_WRITE, AUTH_REL_CAN_ADMIN)
    val ADMIN_RIGHTS: Set<String> = setOf(AUTH_REL_CAN_ADMIN)
    val WRITE_RIGHTS: Set<String> = setOf(AUTH_REL_CAN_WRITE).plus(ADMIN_RIGHTS)
    val READ_RIGHTS: Set<String> = setOf(AUTH_REL_CAN_READ).plus(WRITE_RIGHTS)

    enum class SpecificAccessPolicy {
        AUTH_READ,
        AUTH_WRITE
    }
}

// sub as per https://openid.net/specs/openid-connect-core-1_0.html#IDToken
typealias Sub = String

suspend fun getSubFromSecurityContext(): Option<Sub> {
    return ReactiveSecurityContextHolder.getContext()
        .switchIfEmpty(Mono.just(SecurityContextImpl()))
        .map { context ->
            context.authentication?.principal?.let { Some((it as Jwt).subject) } ?: None
        }
        .awaitFirst()
}

fun Option<Sub>.toStringValue(): String = this.getOrElse { "" }

fun URI.extractSub(): Sub =
    this.toString().extractSub()

fun String.extractSub(): Sub =
    this.substringAfterLast(":")

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
        fun forAttributeName(attributeName: String): Option<AccessRight> =
            values().find { it.attributeName == attributeName }.toOption()

        fun forExpandedAttributeName(attributeName: ExpandedTerm): Option<AccessRight> =
            when (attributeName) {
                AUTH_REL_CAN_READ -> R_CAN_READ.some()
                AUTH_REL_CAN_WRITE -> R_CAN_WRITE.some()
                AUTH_REL_CAN_ADMIN -> R_CAN_ADMIN.some()
                else -> None
            }
    }
}
