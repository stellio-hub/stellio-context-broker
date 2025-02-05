package com.egm.stellio.search.csr.service

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.csr.model.CSRFilters
import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.search.csr.model.Mode
import com.egm.stellio.search.csr.model.Operation
import com.egm.stellio.search.csr.model.RegistrationInfoFilter
import com.egm.stellio.search.entity.web.BatchEntityError
import com.egm.stellio.search.entity.web.BatchEntitySuccess
import com.egm.stellio.search.entity.web.BatchOperationResult
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadGatewayException
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.ConflictException
import com.egm.stellio.shared.model.ContextSourceException
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.GatewayTimeoutException
import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.compactEntity
import com.egm.stellio.shared.util.toUri
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
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
        entity: ExpandedEntity,
        contexts: List<String>,
    ): Pair<BatchOperationResult, ExpandedEntity?> {
        val csrFilters =
            CSRFilters(
                ids = setOf(entity.id.toUri()),
                types = entity.types.toSet()
            )
        val result = BatchOperationResult()
        val registrationInfoFilter =
            RegistrationInfoFilter(ids = setOf(entity.id.toUri()), types = entity.types.toSet())

        val matchingCSR = contextSourceRegistrationService.getContextSourceRegistrations(
            filters = csrFilters,
        ).groupBy { it.mode }

        val entityAfterExclusive = distributeCreateEntityForContextSources(
            matchingCSR[Mode.EXCLUSIVE], // can only be one
            registrationInfoFilter,
            entity,
            contexts,
            result
        )
        if (entityAfterExclusive == null) return result to null

        val entityAfterRedirect = distributeCreateEntityForContextSources(
            matchingCSR[Mode.REDIRECT],
            registrationInfoFilter,
            entityAfterExclusive,
            contexts,
            result
        )
        if (entityAfterRedirect == null) return result to null

        distributeCreateEntityForContextSources(
            matchingCSR[Mode.INCLUSIVE],
            registrationInfoFilter,
            entityAfterRedirect,
            contexts,
            result
        )
        return result to entityAfterRedirect
    }

    internal suspend fun distributeCreateEntityForContextSources(
        csrs: List<ContextSourceRegistration>?,
        registrationInfoFilter: RegistrationInfoFilter,
        entity: ExpandedEntity,
        contexts: List<String>,
        resultToUpdate: BatchOperationResult
    ): ExpandedEntity? {
        val allProcessedAttrs = mutableSetOf<ExpandedTerm>()
        csrs?.forEach { csr ->
            csr.getAssociatedAttributes(registrationInfoFilter, entity)
                .let { attrs ->
                    allProcessedAttrs.addAll(attrs)
                    if (attrs.isEmpty()) Unit
                    else if (csr.operations.any {
                            it == Operation.CREATE_ENTITY ||
                                it == Operation.UPDATE_OPS ||
                                it == Operation.REDIRECTION_OPS
                        }
                    ) {
                        postDistributedInformation(
                            compactEntity(entity.filterAttributes(attrs, emptySet()), contexts),
                            csr,
                            createPath
                        ).fold(
                            {
                                resultToUpdate.errors.add(
                                    BatchEntityError(
                                        entityId = entity.id.toUri(),
                                        registrationId = csr.id,
                                        error = it.toProblemDetail()
                                    )
                                )
                            },
                            { resultToUpdate.success.add(BatchEntitySuccess(csr.id)) }
                        )
                    } else if (csr.mode != Mode.INCLUSIVE) {
                        resultToUpdate.errors.add(
                            BatchEntityError(
                                entityId = entity.id.toUri(),
                                registrationId = csr.id,
                                error = ConflictException(
                                    "The csr: ${csr.id} does not support the creation of entities"
                                ).toProblemDetail()
                            )
                        )
                    }
                }
        }
        return if (allProcessedAttrs.isNotEmpty()) {
            val remainingEntity = entity.omitAttributes(allProcessedAttrs)
            if (remainingEntity.hasNonCoreAttributes()) remainingEntity else null
        } else entity
    }

    internal suspend fun postDistributedInformation(
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
                newHeaders[HttpHeaders.CONTENT_TYPE] = JSON_LD_CONTENT_TYPE
            }.bodyValue(entity)

        return runCatching {
            val (statusCode, response, _) = request.awaitExchange { response ->
                Triple(response.statusCode(), response.awaitBodyOrNull<String>(), response.headers())
            }
            if (statusCode.value() == HttpStatus.MULTI_STATUS.value()) {
                ContextSourceException(
                    type = URI("https://uri.etsi.org/ngsi-ld/errors/MultiStatus"),
                    status = HttpStatus.MULTI_STATUS,
                    title = "Context source returned 207",
                    detail = response ?: "no message"
                ).left()
            } else if (statusCode.is2xxSuccessful) {
                logger.info("Successfully post data to CSR ${csr.id} at $uri")
                Unit.right()
            } else if (response == null) {
                val message = "No error message received from CSR ${csr.id} at $uri"
                logger.warn(message)
                BadGatewayException(message).left()
            } else {
                logger.warn("Error creating an entity for CSR at $uri: $response")
                ContextSourceException.fromResponse(response).left()
            }
        }.fold(
            onSuccess = { it },
            onFailure = { e ->
                logger.warn("Error contacting CSR at $uri: ${e.message}")
                logger.warn(e.stackTraceToString())
                GatewayTimeoutException(
                    "Error connecting to CSR at $uri: \"${e.cause}:${e.message}\""
                ).left()
            }
        )
    }
}
