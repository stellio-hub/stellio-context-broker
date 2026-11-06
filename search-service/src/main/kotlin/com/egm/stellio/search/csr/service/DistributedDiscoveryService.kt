package com.egm.stellio.search.csr.service

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.raise.catch
import arrow.core.right
import arrow.core.separateEither
import arrow.fx.coroutines.parMap
import com.egm.stellio.search.csr.model.CSRFilters
import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.search.csr.model.NGSILDWarning
import com.egm.stellio.search.csr.model.Operation
import com.egm.stellio.search.csr.model.RevalidationFailedWarning
import com.egm.stellio.search.csr.util.ContextSourceHttpUtils
import com.egm.stellio.search.csr.util.restrictTo
import com.egm.stellio.search.discovery.model.AttributeDetails
import com.egm.stellio.search.discovery.model.AttributeList
import com.egm.stellio.search.discovery.model.AttributeTypeInfo
import com.egm.stellio.search.discovery.model.EntityType
import com.egm.stellio.search.discovery.model.EntityTypeInfo
import com.egm.stellio.search.discovery.model.EntityTypeList
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.queryparameter.QP
import com.egm.stellio.shared.util.ErrorMessages.Csr.contextSourceBadlyFormedMessage
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.stereotype.Service
import org.springframework.util.LinkedMultiValueMap
import org.springframework.util.MultiValueMap
import java.net.URI

typealias DistributedResults<T> = Pair<List<NGSILDWarning>, List<T>>

suspend fun <T> distributeIfNotLocal(
    local: Boolean,
    distribute: suspend () -> DistributedResults<T>
): DistributedResults<T> =
    if (local) emptyList<NGSILDWarning>() to emptyList()
    else distribute()

/**
 * Forwards the Discovery operations (TS 104 175 §10.6) to the matching Context Sources.
 *
 * Each answer is restricted to the types and attributes declared in the information of the CSR it comes from, so that
 * a Context Source never exposes more than what it registered.
 */
@Service
class DistributedDiscoveryService(
    private val contextSourceRegistrationService: ContextSourceRegistrationService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    suspend fun distributeRetrieveEntityTypes(
        httpHeaders: HttpHeaders,
        contexts: List<String>
    ): DistributedResults<EntityTypeList> =
        distribute(
            Operation.RETRIEVE_ENTITY_TYPES.toCSRFilters(),
            DiscoveryRequest(TYPES_PATH),
            httpHeaders,
            parse = { deserializeAs<EntityTypeList>(it) }
        ) { entityTypeList, csr -> entityTypeList.restrictTo(csr, contexts) }

    suspend fun distributeRetrieveEntityTypeDetails(
        httpHeaders: HttpHeaders,
        contexts: List<String>
    ): DistributedResults<List<EntityType>> =
        distribute(
            Operation.RETRIEVE_ENTITY_TYPE_DETAILS.toCSRFilters(),
            DiscoveryRequest(TYPES_PATH, details = true),
            httpHeaders,
            parse = { deserializeAs<List<EntityType>>(it) }
        ) { entityTypes, csr -> entityTypes.restrictTo(csr, contexts) }

    suspend fun distributeRetrieveEntityTypeInfo(
        type: ExpandedTerm,
        httpHeaders: HttpHeaders,
        contexts: List<String>
    ): DistributedResults<EntityTypeInfo> =
        distribute(
            Operation.RETRIEVE_ENTITY_TYPE_INFO.toCSRFilters(type = type),
            DiscoveryRequest(TYPES_PATH, lastPathSegment = type),
            httpHeaders,
            parse = { deserializeAs<EntityTypeInfo>(it) }
        ) { entityTypeInfo, csr -> entityTypeInfo.restrictTo(type, csr, contexts) }

    suspend fun distributeRetrieveAttributes(
        httpHeaders: HttpHeaders,
        contexts: List<String>
    ): DistributedResults<AttributeList> =
        distribute(
            Operation.RETRIEVE_ATTR_TYPES.toCSRFilters(),
            DiscoveryRequest(ATTRIBUTES_PATH),
            httpHeaders,
            parse = { deserializeAs<AttributeList>(it) }
        ) { attributeList, csr -> attributeList.restrictTo(csr, contexts) }

    suspend fun distributeRetrieveAttributeDetails(
        httpHeaders: HttpHeaders,
        contexts: List<String>
    ): DistributedResults<List<AttributeDetails>> =
        distribute(
            Operation.RETRIEVE_ATTR_TYPE_DETAILS.toCSRFilters(),
            DiscoveryRequest(ATTRIBUTES_PATH, details = true),
            httpHeaders,
            parse = { deserializeAs<List<AttributeDetails>>(it) }
        ) { attributes, csr -> attributes.restrictTo(csr, contexts) }

    suspend fun distributeRetrieveAttributeTypeInfo(
        attribute: ExpandedTerm,
        httpHeaders: HttpHeaders,
        contexts: List<String>
    ): DistributedResults<AttributeTypeInfo> =
        distribute(
            Operation.RETRIEVE_ATTR_TYPE_INFO.toCSRFilters(attrs = setOf(attribute)),
            DiscoveryRequest(ATTRIBUTES_PATH, lastPathSegment = attribute),
            httpHeaders,
            parse = { deserializeAs<AttributeTypeInfo>(it) }
        ) { attributeTypeInfo, csr -> attributeTypeInfo.restrictTo(attribute, csr, contexts) }

    /**
     * Calls each matching CSR once (not once per EntityInfo, the answer being global to the Context Source).
     * A 404 from the Context Source is ignored, as is an answer restricted to nothing (restrict returning null).
     */
    private suspend fun <T : Any> distribute(
        csrFilters: CSRFilters,
        request: DiscoveryRequest,
        httpHeaders: HttpHeaders,
        parse: (String) -> T,
        restrict: (T, ContextSourceRegistration) -> T?
    ): DistributedResults<T> =
        contextSourceRegistrationService.getContextSourceRegistrations(csrFilters).parMap { csr ->
            val response = ContextSourceHttpUtils.callGetOnCsr(
                URI("${csr.endpoint}${request.path}"),
                httpHeaders,
                request.queryParams(),
                csr,
                request.lastPathSegment
            )
            contextSourceRegistrationService.updateContextSourceStatus(csr, response.isRight())
            response.flatMap { (body, _) ->
                if (body == null) null.right()
                else parseRemotePayload(body, csr, request.path, parse).map { restrict(it, csr) }
            }
        }.separateEither().let { (warnings, results) -> warnings to results.filterNotNull() }

    private fun <T : Any> parseRemotePayload(
        body: String,
        csr: ContextSourceRegistration,
        path: String,
        parse: (String) -> T
    ): Either<NGSILDWarning, T> =
        catch(
            { parse(body).right() },
            { e ->
                logger.warn("Badly formed data received from CSR ${csr.id} at $path: ${e.message}")
                RevalidationFailedWarning(
                    contextSourceBadlyFormedMessage(csr.id, path, e.cause.toString(), e.message),
                    csr
                ).left()
            }
        )

    private fun Operation.toCSRFilters(type: ExpandedTerm? = null, attrs: Set<ExpandedTerm> = emptySet()) =
        CSRFilters(typeSelection = type, operations = getMatchingOperations().toList(), attrs = attrs)

    private data class DiscoveryRequest(
        val path: String,
        val lastPathSegment: String? = null,
        val details: Boolean = false
    ) {
        fun queryParams(): MultiValueMap<String, String> =
            LinkedMultiValueMap<String, String>().apply { if (details) add(QP.DETAILS.key, "true") }
    }

    companion object {
        private const val TYPES_PATH = "/ngsi-ld/v1/types"
        private const val ATTRIBUTES_PATH = "/ngsi-ld/v1/attributes"
    }
}
