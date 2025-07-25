package com.egm.stellio.shared.util

import arrow.core.Either
import arrow.core.None
import arrow.core.Option
import arrow.core.getOrElse
import arrow.core.left
import arrow.core.right
import arrow.core.some
import arrow.core.toOption
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.NgsiLdAttribute
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.model.NgsiLdPropertyInstance
import com.egm.stellio.shared.util.AuthContextModel.AUTHORIZATION_ONTOLOGY
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_ADMIN
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_READ
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_WRITE
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_IS_OWNER
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

    const val DATASET_ID_PREFIX = "urn:ngsi-ld:Dataset:"

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
    const val AUTH_TERM_RIGHT = "right"
    const val AUTH_PROP_RIGHT: ExpandedTerm = AUTHORIZATION_ONTOLOGY + AUTH_TERM_RIGHT
    const val AUTH_TERM_IS_DELETED = "isDeleted"
    const val AUTH_PROP_IS_DELETED: ExpandedTerm = AUTHORIZATION_ONTOLOGY + AUTH_TERM_IS_DELETED

    const val AUTH_TERM_IS_MEMBER_OF = "isMemberOf"
    const val AUTH_REL_IS_MEMBER_OF: ExpandedTerm = AUTHORIZATION_ONTOLOGY + AUTH_TERM_IS_MEMBER_OF
    const val AUTH_TERM_CAN_READ = "canRead"
    const val AUTH_REL_CAN_READ: ExpandedTerm = AUTHORIZATION_ONTOLOGY + AUTH_TERM_CAN_READ
    const val AUTH_TERM_CAN_WRITE = "canWrite"
    const val AUTH_REL_CAN_WRITE: ExpandedTerm = AUTHORIZATION_ONTOLOGY + AUTH_TERM_CAN_WRITE
    const val AUTH_TERM_CAN_ADMIN = "canAdmin"
    const val AUTH_REL_CAN_ADMIN: ExpandedTerm = AUTHORIZATION_ONTOLOGY + AUTH_TERM_CAN_ADMIN
    const val AUTH_TERM_IS_OWNER = "isOwner"
    const val AUTH_REL_IS_OWNER: ExpandedTerm = AUTHORIZATION_ONTOLOGY + AUTH_TERM_IS_OWNER
    val ALL_IAM_RIGHTS_TERMS = setOf(AUTH_TERM_CAN_READ, AUTH_TERM_CAN_WRITE, AUTH_TERM_CAN_ADMIN, AUTH_TERM_IS_OWNER)
    val ALL_IAM_RIGHTS = setOf(AUTH_REL_CAN_READ, AUTH_REL_CAN_WRITE, AUTH_REL_CAN_ADMIN, AUTH_REL_IS_OWNER)
    val ALL_ASSIGNABLE_IAM_RIGHTS = ALL_IAM_RIGHTS.minus(AUTH_REL_IS_OWNER)

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

fun NgsiLdEntity.getSpecificAccessPolicy(): Either<APIException, AuthContextModel.SpecificAccessPolicy>? =
    this.properties.find { it.name == AuthContextModel.AUTH_PROP_SAP }?.getSpecificAccessPolicy()

fun NgsiLdAttribute.getSpecificAccessPolicy(): Either<APIException, AuthContextModel.SpecificAccessPolicy> {
    val ngsiLdAttributeInstances = this.getAttributeInstances()
    if (ngsiLdAttributeInstances.size > 1)
        return BadRequestDataException("Payload must contain a single attribute instance").left()
    val ngsiLdAttributeInstance = ngsiLdAttributeInstances[0]
    if (ngsiLdAttributeInstance !is NgsiLdPropertyInstance)
        return BadRequestDataException("Payload must be a property").left()
    return try {
        AuthContextModel.SpecificAccessPolicy.valueOf(ngsiLdAttributeInstance.value.toString()).right()
    } catch (e: java.lang.IllegalArgumentException) {
        BadRequestDataException("Value must be one of AUTH_READ or AUTH_WRITE", e.message).left()
    }
}

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

enum class AccessRight(val attributeName: String) {
    CAN_READ("canRead"),
    CAN_WRITE("canWrite"),
    CAN_ADMIN("canAdmin"),
    IS_OWNER("isOwner");

    companion object {
        fun forAttributeName(attributeName: String): Option<AccessRight> =
            entries.find { it.attributeName == attributeName.removePrefix(AUTHORIZATION_ONTOLOGY) }.toOption()

        fun forExpandedAttributeName(attributeName: ExpandedTerm): Option<AccessRight> =
            when (attributeName) {
                AUTH_REL_CAN_READ -> CAN_READ.some()
                AUTH_REL_CAN_WRITE -> CAN_WRITE.some()
                AUTH_REL_CAN_ADMIN -> CAN_ADMIN.some()
                AUTH_REL_IS_OWNER -> IS_OWNER.some()
                else -> None
            }
    }
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
