package com.egm.stellio.search.csr.service

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.catch
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.csr.model.CSRFilters
import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.search.csr.model.EntityInfo.Companion.addFilterForEntityInfo
import com.egm.stellio.search.csr.model.Mode
import com.egm.stellio.search.csr.model.Operation
import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
import com.egm.stellio.search.entity.web.BatchOperationResult
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadGatewayException
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.ConflictException
import com.egm.stellio.shared.model.ContextSourceException
import com.egm.stellio.shared.model.EntityTypeSelection
import com.egm.stellio.shared.model.ErrorType
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.GatewayTimeoutException
import com.egm.stellio.shared.queryparameter.QP
import com.egm.stellio.shared.util.DataTypes.deserializeAs
import com.egm.stellio.shared.util.ErrorMessages.Csr.CONTEXT_SOURCE_MULTISTATUS_MESSAGE
import com.egm.stellio.shared.util.ErrorMessages.Csr.CSR_IDPATTERN_CONFLICT_MESSAGE
import com.egm.stellio.shared.util.ErrorMessages.Csr.contextSourceContactErrorMessage
import com.egm.stellio.shared.util.ErrorMessages.Csr.contextSourceNoErrorMessage
import com.egm.stellio.shared.util.ErrorMessages.Csr.csrDoesNotSupportCreationMessage
import com.egm.stellio.shared.util.ErrorMessages.Csr.csrDoesNotSupportDeletionMessage
import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.compactEntity
import com.egm.stellio.shared.util.toTypeSelection
import com.egm.stellio.shared.util.toUri
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.HttpStatusCode
import org.springframework.stereotype.Service
import org.springframework.util.MultiValueMap
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBodyOrNull
import org.springframework.web.reactive.function.client.awaitExchange
import java.net.URI

@Service
class DistributedEntityProvisionService(
    private val contextSourceRegistrationService: ContextSourceRegistrationService
) {
    val entityPath = "/ngsi-ld/v1/entities"

    val logger: Logger = LoggerFactory.getLogger(javaClass)

    suspend fun distributeDeleteEntity(
        entityId: URI,
        queryParams: MultiValueMap<String, String>
    ): BatchOperationResult {
        val csrFilters = CSRFilters(ids = setOf(entityId), typeSelection = queryParams.getFirst(QP.TYPE.key))
        val result = BatchOperationResult()
        val matchingCSR = contextSourceRegistrationService.getContextSourceRegistrations(filters = csrFilters)

        matchingCSR.filter { it.mode != Mode.AUXILIARY }
            .forEach { csr ->
                if (csr.isMatchingOperation(Operation.DELETE_ENTITY)) {
                    result.addEither(
                        sendDistributedInformation(null, csr, "$entityPath/$entityId", HttpMethod.DELETE),
                        entityId,
                        csr.id
                    )
                } else if (csr.mode != Mode.INCLUSIVE) {
                    result.addEither(
                        ConflictException(csrDoesNotSupportDeletionMessage(csr.id)).left(),
                        entityId,
                        csr.id
                    )
                }
            }

        return result
    }

    suspend fun distributePurgeEntities(
        httpHeaders: HttpHeaders,
        entitiesQuery: EntitiesQueryFromGet,
        queryParams: MultiValueMap<String, String>
    ): Either<APIException, BatchOperationResult> {
        // TODO use keep and drop to match CSRs??
        val csrFilters =
            CSRFilters(
                ids = entitiesQuery.ids,
                idPattern = entitiesQuery.idPattern,
                typeSelection = entitiesQuery.typeSelection,
                operations = Operation.PURGE_ENTITY.getMatchingOperations().toList()
            )

        val matchingCSR = contextSourceRegistrationService.getContextSourceRegistrations(filters = csrFilters)
            .flatMap { it.toSingleEntityInfoCSRList(csrFilters) }

        val result = BatchOperationResult()

        matchingCSR.filter { it.mode != Mode.AUXILIARY }
            .forEach { csr ->
                val entityInfo = csr.information.first().entities
                if (entitiesQuery.idPattern != null && entityInfo?.any { it.idPattern != null } == true)
                    result.addEither(
                        ConflictException(CSR_IDPATTERN_CONFLICT_MESSAGE).left(),
                        "urn:ngsi-ld:*".toUri(),
                        csr.id
                    )
                else {
                    val newParams = entityInfo?.first()?.let { queryParams.addFilterForEntityInfo(it) } ?: queryParams

                    sendDistributedPurgeOperation(httpHeaders, csr, newParams).fold({
                        result.addEither(it.left(), "urn:ngsi-ld:*".toUri(), csr.id)
                    }, {
                        if (it.second == HttpStatus.NO_CONTENT) {
                            result.addEither(Unit.right(), "urn:ngsi-ld:*".toUri(), csr.id)
                        } else if (it.second == HttpStatus.MULTI_STATUS) {
                            result += deserializeAs<BatchOperationResult>(it.first!!)
                        }
                    })
                }
            }

        return result.right()
    }

    suspend fun distributeCreateEntity(
        entity: ExpandedEntity,
        contexts: List<String>,
    ): Pair<BatchOperationResult, ExpandedEntity?> = distributeEntityProvision(
        entity,
        contexts,
        Operation.CREATE_ENTITY,
        entity.types.toTypeSelection()
    )

    suspend fun distributeReplaceEntity(
        entity: ExpandedEntity,
        contexts: List<String>,
        queryParams: MultiValueMap<String, String>
    ): Pair<BatchOperationResult, ExpandedEntity?> = distributeEntityProvision(
        entity,
        contexts,
        Operation.REPLACE_ENTITY,
        queryParams.getFirst(QP.TYPE.key) ?: entity.types.toTypeSelection()
    )

    suspend fun distributeEntityProvision(
        entity: ExpandedEntity,
        contexts: List<String>,
        operation: Operation,
        typeSelection: EntityTypeSelection
    ): Pair<BatchOperationResult, ExpandedEntity?> {
        val csrFilters =
            CSRFilters(
                ids = setOf(entity.id),
                typeSelection = typeSelection
            )
        val result = BatchOperationResult()

        val matchingCSR = contextSourceRegistrationService.getContextSourceRegistrations(
            filters = csrFilters,
        ).groupBy { it.mode }

        val entityAfterExclusive = distributeEntityProvisionForContextSources(
            matchingCSR[Mode.EXCLUSIVE],
            csrFilters,
            entity,
            contexts,
            result,
            operation
        )
        if (entityAfterExclusive == null) return result to null

        val entityAfterRedirect = distributeEntityProvisionForContextSources(
            matchingCSR[Mode.REDIRECT],
            csrFilters,
            entityAfterExclusive,
            contexts,
            result,
            operation
        )
        if (entityAfterRedirect == null) return result to null

        distributeEntityProvisionForContextSources(
            matchingCSR[Mode.INCLUSIVE],
            csrFilters,
            entityAfterRedirect,
            contexts,
            result,
            operation
        )
        return result to entityAfterRedirect
    }

    internal suspend fun distributeEntityProvisionForContextSources(
        csrs: List<ContextSourceRegistration>?,
        csrFilters: CSRFilters,
        entity: ExpandedEntity,
        contexts: List<String>,
        resultToUpdate: BatchOperationResult,
        operation: Operation,
    ): ExpandedEntity? {
        val allProcessedAttrs = mutableSetOf<ExpandedTerm>()
        csrs?.forEach { csr ->
            csr.getAttributesMatchingCSFAndEntity(csrFilters, entity)
                .let { attrs ->
                    allProcessedAttrs.addAll(attrs)
                    if (attrs.isEmpty()) Unit
                    else if (csr.isMatchingOperation(operation)) {
                        resultToUpdate.addEither(
                            sendDistributedInformation(
                                compactEntity(entity.filterAttributes(attrs), contexts),
                                csr,
                                operation.getPath(entity.id),
                                operation.method!!
                            ),
                            entity.id,
                            csr.id
                        )
                    } else if (csr.mode != Mode.INCLUSIVE) {
                        resultToUpdate.addEither(
                            ConflictException(csrDoesNotSupportCreationMessage(csr.id)).left(),
                            entity.id,
                            csr.id
                        )
                    }
                }
        }
        return if (allProcessedAttrs.isNotEmpty()) {
            val remainingEntity = entity.omitAttributes(allProcessedAttrs)
            if (remainingEntity.hasNonCoreAttributes()) remainingEntity else null
        } else entity
    }

    internal suspend fun sendDistributedInformation(
        body: CompactedEntity?,
        csr: ContextSourceRegistration,
        path: String,
        method: HttpMethod,
        queryParams: MultiValueMap<String, String> = MultiValueMap.fromSingleValue(emptyMap())
    ): Either<APIException, Unit> = either {
        val uri = URI("${csr.endpoint}$path")

        val request = WebClient.create()
            .method(method)
            .uri { uriBuilder ->
                uriBuilder.scheme(uri.scheme)
                    .host(uri.host)
                    .port(uri.port)
                    .path(uri.path)
                    .queryParams(queryParams)
                    .build()
            }.headers { newHeaders ->
                newHeaders[HttpHeaders.CONTENT_TYPE] = JSON_LD_CONTENT_TYPE
            }

        body?.let { request.bodyValue(body) }

        return catch(
            {
                val (statusCode, response, _) = request.awaitExchange { response ->
                    Triple(response.statusCode(), response.awaitBodyOrNull<String>(), response.headers())
                }
                if (statusCode.value() == HttpStatus.MULTI_STATUS.value()) {
                    ContextSourceException(
                        type = ErrorType.MULTI_STATUS.type,
                        status = HttpStatus.MULTI_STATUS,
                        title = CONTEXT_SOURCE_MULTISTATUS_MESSAGE,
                        detail = response ?: "No message"
                    ).left()
                } else if (statusCode.is2xxSuccessful) {
                    logger.info("Successfully post data to CSR ${csr.id} at $uri")
                    Unit.right()
                } else if (response == null) {
                    val message = contextSourceNoErrorMessage(csr.id, uri)
                    logger.warn(message)
                    BadGatewayException(message).left()
                } else {
                    logger.warn("Error creating an entity for CSR at $uri: $response")
                    ContextSourceException.fromResponse(response).left()
                }
            },
            { e ->
                logger.warn("Error contacting CSR at $uri: ${e.message}")
                logger.warn(e.stackTraceToString())
                GatewayTimeoutException(
                    contextSourceContactErrorMessage(csr.id, uri),
                    detail = "${e.cause}: ${e.message}"
                ).left()
            }
        )
    }

    internal suspend fun sendDistributedPurgeOperation(
        httpHeaders: HttpHeaders,
        csr: ContextSourceRegistration,
        queryParams: MultiValueMap<String, String> = MultiValueMap.fromSingleValue(emptyMap())
    ): Either<APIException, Pair<String?, HttpStatusCode>> = either {
        val uri = URI("${csr.endpoint}$entityPath")

        val request = WebClient.create()
            .method(HttpMethod.DELETE)
            .uri { uriBuilder ->
                uriBuilder.scheme(uri.scheme)
                    .host(uri.host)
                    .port(uri.port)
                    .path(uri.path)
                    .queryParams(queryParams)
                    .build()
            }.headers { newHeaders ->
                httpHeaders.getFirst(HttpHeaders.LINK)?.let { link -> newHeaders[HttpHeaders.LINK] = link }
            }

        return catch(
            {
                val (statusCode, response, _) = request.awaitExchange { response ->
                    Triple(response.statusCode(), response.awaitBodyOrNull<String>(), response.headers())
                }
                if (statusCode.is2xxSuccessful) {
                    logger.info("Successfully sent data to CSR ${csr.id} at $uri")
                    (response to statusCode).right()
                } else if (response == null) {
                    val message = contextSourceNoErrorMessage(csr.id, uri)
                    logger.warn(message)
                    BadGatewayException(message).left()
                } else {
                    logger.warn("Error purging entities for CSR at $uri: $response")
                    ContextSourceException.fromResponse(response).left()
                }
            },
            { e ->
                logger.warn("Error contacting CSR at $uri: ${e.message}")
                logger.warn(e.stackTraceToString())
                GatewayTimeoutException(
                    contextSourceContactErrorMessage(csr.id, uri),
                    detail = "${e.cause}: ${e.message}"
                ).left()
            }
        )
    }
}
