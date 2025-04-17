package com.egm.stellio.shared.util

import arrow.core.Either
import arrow.core.Option
import arrow.core.getOrElse
import arrow.core.raise.either
import arrow.core.toOption
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.util.GlobalRole.STELLIO_ADMIN
import com.egm.stellio.shared.util.GlobalRole.STELLIO_CREATOR
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.security.core.context.ReactiveSecurityContextHolder
import org.springframework.security.core.context.SecurityContextImpl
import reactor.core.publisher.Mono
import java.net.URI

val ADMIN_ROLES: Set<GlobalRole> = setOf(STELLIO_ADMIN)
val CREATION_ROLES: Set<GlobalRole> = setOf(STELLIO_CREATOR).plus(ADMIN_ROLES)

object AuthContextModel {
    const val AUTHORIZATION_ONTOLOGY = "https://ontology.eglobalmark.com/authorization#"

    const val USER_COMPACT_TYPE = "User"
    const val USER_TYPE: ExpandedTerm = AUTHORIZATION_ONTOLOGY + USER_COMPACT_TYPE
    const val GROUP_COMPACT_TYPE = "Group"
    const val GROUP_TYPE: ExpandedTerm = AUTHORIZATION_ONTOLOGY + GROUP_COMPACT_TYPE
    const val CLIENT_COMPACT_TYPE = "Client"

    const val USER_ENTITY_PREFIX = "urn:ngsi-ld:User:"
    const val CLIENT_ENTITY_PREFIX = "urn:ngsi-ld:Client:"
    const val GROUP_ENTITY_PREFIX = "urn:ngsi-ld:Group:"

    const val AUTH_PERMISSION_TERM = "Permission"
    const val AUTH_ASSIGNEE_TERM = "assignee"
    const val AUTH_ASSIGNER_TERM = "assigner"
    const val AUTH_ACTION_TERM = "action"
    const val AUTH_TARGET_TERM = "target"
    const val AUTH_TERM_SUB = "sub"
    const val AUTH_PROP_SUB = AUTHORIZATION_ONTOLOGY + AUTH_TERM_SUB
    const val AUTH_TERM_CLIENT_ID = "clientId"
    const val AUTH_TERM_NAME = "name"
    const val AUTH_PROP_NAME = "https://schema.org/$AUTH_TERM_NAME"
    const val AUTH_TERM_SID = "serviceAccountId"
    const val AUTH_TERM_SUBJECT_INFO = "subjectInfo"
    const val AUTH_PROP_SUBJECT_INFO = AUTHORIZATION_ONTOLOGY + AUTH_TERM_SUBJECT_INFO
    const val AUTH_TERM_ROLES = "roles"
    const val AUTH_TERM_KIND = "kind"
    const val AUTH_TERM_USERNAME = "username"
    const val AUTH_PROP_USERNAME = AUTHORIZATION_ONTOLOGY + AUTH_TERM_USERNAME
    const val AUTH_TERM_GIVEN_NAME = "givenName"
    const val AUTH_PROP_GIVEN_NAME = AUTHORIZATION_ONTOLOGY + AUTH_TERM_GIVEN_NAME
    const val AUTH_TERM_FAMILY_NAME = "familyName"
    const val AUTH_PROP_FAMILY_NAME = AUTHORIZATION_ONTOLOGY + AUTH_TERM_FAMILY_NAME
    const val AUTH_TERM_SAP = "specificAccessPolicy"
    const val AUTH_PROP_SAP = AUTHORIZATION_ONTOLOGY + AUTH_TERM_SAP

    const val AUTH_TERM_IS_MEMBER_OF = "isMemberOf"
    const val AUTH_REL_IS_MEMBER_OF: ExpandedTerm = AUTHORIZATION_ONTOLOGY + AUTH_TERM_IS_MEMBER_OF
}

// sub as per https://openid.net/specs/openid-connect-core-1_0.html#IDToken
typealias Sub = String

suspend fun getSubFromSecurityContext(): Option<Sub> {
    return ReactiveSecurityContextHolder.getContext()
        .switchIfEmpty(Mono.just(SecurityContextImpl()))
        .map { context ->
            // Authentication#getName maps to the JWT’s sub property, if one is present.
            context.authentication?.name.toOption()
        }
        .awaitFirst()
}

suspend fun getNullableSubFromSecurityContext(): Sub? =
    getSubFromSecurityContext().getOrElse { null }

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
            entries.find { it.key == key }.toOption()
    }
}

fun getAuthzContextFromRequestOrDefault(
    httpHeaders: HttpHeaders,
    body: Map<String, Any>,
    contexts: ApplicationProperties.Contexts
): Either<APIException, List<String>> = either {
    checkAndGetContext(httpHeaders, body, contexts.core).bind()
        .replaceDefaultContextToAuthzContext(contexts)
}

fun getAuthzContextFromLinkHeaderOrDefault(
    httpHeaders: HttpHeaders,
    contexts: ApplicationProperties.Contexts
): Either<APIException, List<String>> =
    getContextFromLinkHeader(httpHeaders.getOrEmpty(HttpHeaders.LINK))
        .map {
            if (it != null)
                if (canExpandJsonLdKeyFromCore(listOf(it)))
                    listOf(it, contexts.authz)
                else listOf(it, contexts.authzCompound)
            else listOf(contexts.authzCompound)
        }

fun List<String>.replaceDefaultContextToAuthzContext(contexts: ApplicationProperties.Contexts) =
    if (this.size == 1 && this[0] == contexts.core)
        listOf(contexts.authzCompound)
    else this
