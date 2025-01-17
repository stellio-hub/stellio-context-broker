package com.egm.stellio.search.csr.service

import arrow.core.Either
import arrow.core.getOrNone
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.search.csr.model.InternalCSRFilters
import com.egm.stellio.search.csr.model.Mode
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadGatewayException
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.ContextSourceException
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.GatewayTimeoutException
import com.egm.stellio.shared.util.JsonLdUtils.compactEntity
import com.egm.stellio.shared.util.toUri
import org.json.XMLTokener.entity
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBodyOrNull
import org.springframework.web.reactive.function.client.awaitExchange
import java.net.URI

@Service
class DistributedEntityProvisionService(
    private val contextSourceRegistrationService: ContextSourceRegistrationService,
) {
    val createPath = "/ngsi-ld/v1/entities"

    val logger: Logger = LoggerFactory.getLogger(javaClass)

    suspend fun distributeCreateEntity(
        httpHeaders: HttpHeaders,
        entity: ExpandedEntity,
        contexts: List<String>,
    ): Pair<List<APIException>, ExpandedEntity> {
        val csrFilters =
            InternalCSRFilters(
                ids = setOf(entity.id.toUri()),
                types = entity.types.toSet()
            )

        val matchingCSR = contextSourceRegistrationService.getContextSourceRegistrations(
            filters = csrFilters,
        ).groupBy { it.mode }

        val (exclusiveErrors, entityAfterExclusive) = distributeCreateEntityForContextSources(
            matchingCSR[Mode.EXCLUSIVE], // could be only one
            csrFilters,
            entity,
            httpHeaders,
            contexts
        )
        val (redirectErrors, entityAfterRedirect) = distributeCreateEntityForContextSources(
            matchingCSR[Mode.REDIRECT],
            csrFilters,
            entityAfterExclusive,
            httpHeaders,
            contexts
        )
        val (inclusiveError, _) = distributeCreateEntityForContextSources(
            matchingCSR[Mode.INCLUSIVE],
            csrFilters,
            entityAfterRedirect,
            httpHeaders,
            contexts
        )
        return exclusiveErrors.toMutableList() +
            redirectErrors.toMutableList() +
            inclusiveError.toMutableList() to entityAfterRedirect
    }

    private suspend fun distributeCreateEntityForContextSources(
        csrs: List<ContextSourceRegistration>?,
        csrFilters: InternalCSRFilters,
        entity: ExpandedEntity,
        headers: HttpHeaders,
        contexts: List<String>
    ): Pair<List<APIException>, ExpandedEntity> {
        val allCreatedAttrs = mutableSetOf<ExpandedTerm>()
        val errors = csrs?.mapNotNull { csr ->
            csr.getMatchingPropertiesAndRelationships(csrFilters)
                .let { (properties, relationships) -> entity.getAssociatedAttributes(properties, relationships) }
                .let { attrs ->
                    if (attrs.isEmpty()) {
                        null
                    } else {
                        postDistributedInformation(
                            headers,
                            compactEntity(entity.filterAttributes(attrs, emptySet()), contexts),
                            csr,
                            createPath
                        ).onRight { allCreatedAttrs.addAll(attrs) }.leftOrNull()
                    }
                }
        } ?: emptyList()
        return errors to entity.omitAttributes(allCreatedAttrs)
    }

    private suspend fun postDistributedInformation(
        httpHeaders: HttpHeaders,
        entity: CompactedEntity,
        csr: ContextSourceRegistration,
        path: String,
    ): Either<APIException, Unit> = either {
        val uri = URI("${csr.endpoint}$path")

        val request = WebClient.create()
            .method(HttpMethod.POST)
            .uri { uriBuilder ->
                uriBuilder.scheme(uri.scheme)
                    .host(uri.host)
                    .port(uri.port)
                    .path(uri.path)
                    .build()
            }.headers { newHeaders ->
                httpHeaders.getOrNone(HttpHeaders.LINK).onSome { link -> newHeaders[HttpHeaders.LINK] = link }
            }.bodyValue(entity)

        return runCatching {
            val (statusCode, response, headers) = request.awaitExchange { response ->
                Triple(response.statusCode(), response.awaitBodyOrNull<String>(), response.headers())
            }

            if (statusCode.is2xxSuccessful) {
                logger.info("Successfully post data to CSR ${csr.id} at $uri")
                Unit.right()
            } else if (response == null) {
                val message = "No error message received from CSR ${csr.id} at $uri"
                logger.warn(message)
                GatewayTimeoutException(message).left()
            } else {
                logger.warn("Error creating an entity for CSR at $uri: $response")
                ContextSourceException(response).left()
            }
        }.fold(
            onSuccess = { it },
            onFailure = { e ->
                logger.warn("Error contacting CSR at $uri: ${e.message}")
                logger.warn(e.stackTraceToString())
                BadGatewayException(
                    "Error connecting to CSR at $uri: \"${e.cause}:${e.message}\""
                ).left()
            }
        )
    }
}
