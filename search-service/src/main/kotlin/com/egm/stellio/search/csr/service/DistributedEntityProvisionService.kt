package com.egm.stellio.search.csr.service

import arrow.core.Either
import arrow.core.getOrNone
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.search.csr.model.InternalCSRFilters
import com.egm.stellio.search.csr.model.Mode
import com.egm.stellio.search.csr.model.Operation
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadGatewayException
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.ConflictException
import com.egm.stellio.shared.model.ContextSourceException
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.GatewayTimeoutException
import com.egm.stellio.shared.util.JsonLdUtils.compactEntity
import com.egm.stellio.shared.util.toUri
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

    val logger: Logger = LoggerFactory.getLogger(javaClass)

    suspend fun distributeCreateEntity(
        httpHeaders: HttpHeaders,
        entity: ExpandedEntity,
        contexts: List<String>,
    ): Pair<List<APIException>, ExpandedEntity> {
        var remainingEntity = entity
        val csrFilters =
            InternalCSRFilters(
                ids = setOf(entity.id.toUri()),
                types = entity.types.toSet()
            )

        val matchingCSR = contextSourceRegistrationService.getContextSourceRegistrations(
            filters = csrFilters,
        ).groupBy { it.mode }

        val exclusiveResponses = matchingCSR[Mode.EXCLUSIVE]?.mapNotNull { csr ->
            createEntityInContextSource(httpHeaders, csr, entity, contexts, csrFilters).fold(
                { it },
                {
                    remainingEntity = it
                    null
                }
            )
        } ?: emptyList() // todo remainingEntity should be treated after all exclusive CSR
        val redirectResponses = matchingCSR[Mode.REDIRECT]?.mapNotNull { csr ->
            createEntityInContextSource(httpHeaders, csr, entity, contexts, csrFilters).fold(
                { it },
                {
                    remainingEntity = it
                    null
                }
            )
        } ?: emptyList()
        val inclusiveResponses = matchingCSR[Mode.INCLUSIVE]?.mapNotNull { csr ->
            createEntityInContextSource(httpHeaders, csr, entity, contexts, csrFilters).leftOrNull()
        } ?: emptyList()
        return exclusiveResponses.toMutableList() +
            redirectResponses.toMutableList() +
            inclusiveResponses.toMutableList() to remainingEntity
    }

    suspend fun createEntityInContextSource(
        httpHeaders: HttpHeaders,
        csr: ContextSourceRegistration,
        entity: ExpandedEntity,
        contexts: List<String>,
        csrFilters: InternalCSRFilters
    ): Either<APIException, ExpandedEntity> = either {
        val path = "/ngsi-ld/v1/entities"
        val matchingInformation = csr.getMatchingInformation(csrFilters)
        val includeAllProperties = matchingInformation.any { it.propertyNames == null }
        val includeAllRelationships = matchingInformation.any { it.relationshipNames == null }
        val attributes = matchingInformation.flatMap { it.propertyNames!! }.toSet()
        val relationships = matchingInformation.flatMap { it.relationshipNames!! }.toSet()

        val matchingEntity = ExpandedEntity(entity.filterAttributes(attributes, emptySet()))
        val remainingEntity = entity
        return if (!csr.operations.any {
                listOf(
                    Operation.CREATE_ENTITY,
                    Operation.REDIRECTION_OPS
                ).contains(it)
            }
        )
            ConflictException("The CSR ${csr.id} does not support creation of entities").left()
        else {
            postDistributedInformation(httpHeaders, compactEntity(matchingEntity, contexts), csr, path)

            remainingEntity.right()
        }
    }

    suspend fun postDistributedInformation(
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
