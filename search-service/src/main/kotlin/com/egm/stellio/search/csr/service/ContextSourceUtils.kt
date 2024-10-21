package com.egm.stellio.search.csr.service

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.csr.model.*
import com.egm.stellio.shared.model.CompactedAttributeInstance
import com.egm.stellio.shared.model.CompactedAttributeInstances
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID_TERM
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SCOPE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.logger
import com.fasterxml.jackson.core.JacksonException
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.util.MultiValueMap
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientException
import org.springframework.web.reactive.function.client.awaitBody
import org.springframework.web.reactive.function.client.awaitExchange
import java.net.URI
import java.time.ZonedDateTime
import kotlin.random.Random.Default.nextBoolean

typealias CompactedEntityWithMode = Pair<CompactedEntity, Mode>
typealias DataSetId = String?
typealias AttributeByDataSetId = Map<DataSetId, CompactedAttributeInstance>
object ContextSourceUtils {

    suspend fun getDistributedInformation(
        httpHeaders: HttpHeaders,
        csr: ContextSourceRegistration,
        path: String,
        params: MultiValueMap<String, String>
    ): Either<NGSILDWarning, CompactedEntity?> = either {
        val uri = URI("${csr.endpoint}$path")

        val request = WebClient.create()
            .method(HttpMethod.GET)
            .uri { uriBuilder ->
                uriBuilder.scheme(uri.scheme)
                    .host(uri.host)
                    .port(uri.port)
                    .path(uri.path)
                    .queryParams(params)
                    .build()
            }
            .header(HttpHeaders.LINK, httpHeaders.getFirst(HttpHeaders.LINK))
        return try {
            val (statusCode, response) = request
                .awaitExchange { response ->
                    response.statusCode() to response.awaitBody<CompactedEntity>()
                }
            when {
                statusCode.is2xxSuccessful -> {
                    logger.info("Successfully received response from CSR at $uri")
                    response.right()
                }
                statusCode.isSameCodeAs(HttpStatus.NOT_FOUND) -> {
                    logger.info("CSR returned 404 at $uri: $response")
                    null.right()
                }
                else -> {
                    logger.warn("Error contacting CSR at $uri: $response")

                    MiscellaneousPersistentWarning(
                        "the CSR ${csr.id} returned an error $statusCode at : $uri response: \"$response\""
                    ).left()
                }
            }
        } catch (e: WebClientException) {
            MiscellaneousWarning(
                "Error connecting to csr ${csr.id} at : $uri message : \"${e.message}\""
            ).left()
        } catch (e: JacksonException) { // todo get the good exception for invalid payload
            RevalidationFailedWarning(
                "the CSR ${csr.id} as : $uri returned badly formed data message: \"${e.message}\""
            ).left()
        }
    }

    fun mergeEntity(
        localEntity: CompactedEntity?,
        remoteEntitiesWithMode: List<CompactedEntityWithMode>
    ): Either<NGSILDWarning, CompactedEntity?> = either {
        if (localEntity == null && remoteEntitiesWithMode.isEmpty()) return@either null

        val mergedEntity = localEntity?.toMutableMap() ?: mutableMapOf()

        remoteEntitiesWithMode.sortedBy { (_, mode) -> mode == Mode.AUXILIARY }
            .forEach { (entity, mode) ->
                entity.entries.forEach {
                        (key, value) ->
                    val mergedValue = mergedEntity[key]
                    when { // todo sysAttrs
                        mergedValue == null -> mergedEntity[key] = value
                        key == JSONLD_ID_TERM || key == JSONLD_CONTEXT -> {}
                        key == JSONLD_TYPE_TERM || key == NGSILD_SCOPE_TERM ->
                            mergedEntity[key] = mergeTypeOrScope(mergedValue, value)
                        else -> mergedEntity[key] = mergeAttribute(
                            mergedValue,
                            value,
                            mode == Mode.AUXILIARY
                        ).bind()
                    }
                }
            }
        mergedEntity
    }

    fun mergeTypeOrScope(
        currentValue: Any, // String || List<String> || Set<String>
        remoteValue: Any
    ) = when {
        currentValue == remoteValue -> currentValue
        currentValue is List<*> && remoteValue is List<*> -> (currentValue.toSet() + remoteValue.toSet()).toList()
        currentValue is List<*> -> (currentValue.toSet() + remoteValue).toList()
        remoteValue is List<*> -> (remoteValue.toSet() + currentValue).toList()
        else -> listOf(currentValue, remoteValue)
    }

    /**
     * Implements 4.5.5 - Multi-Attribute Support
     */
    fun mergeAttribute(
        currentAttribute: Any,
        remoteAttribute: Any,
        isAuxiliary: Boolean = false
    ): Either<NGSILDWarning, Any> = either {
        val currentInstances = groupInstancesByDataSetId(currentAttribute).bind().toMutableMap()
        val remoteInstances = groupInstancesByDataSetId(remoteAttribute).bind()
        remoteInstances.entries.forEach { (datasetId, remoteInstance) ->
            val currentInstance = currentInstances[datasetId]
            when {
                currentInstance == null -> currentInstances[datasetId] = remoteInstance
                isAuxiliary -> {}
                currentInstance.isBefore(remoteInstance, NGSILD_OBSERVED_AT_TERM) ->
                    currentInstances[datasetId] = remoteInstance
                remoteInstance.isBefore(currentInstance, NGSILD_OBSERVED_AT_TERM) -> {}
                currentInstance.isBefore(remoteInstance, NGSILD_MODIFIED_AT_TERM) ->
                    currentInstances[datasetId] = remoteInstance
                remoteInstance.isBefore(currentInstance, NGSILD_MODIFIED_AT_TERM) -> {}
                // if there is no discriminating factor choose one at random
                nextBoolean() -> currentInstances[datasetId] = remoteInstance
                else -> {}
            }
        }
        val values = currentInstances.values.toList()
        if (values.size == 1) values[0] else values
    }

    // do not work with CORE MEMBER since they are nor list nor map
    private fun groupInstancesByDataSetId(attribute: Any): Either<NGSILDWarning, AttributeByDataSetId> =
        when (attribute) {
            is Map<*, *> -> {
                attribute as CompactedAttributeInstance
                mapOf(attribute[NGSILD_DATASET_ID_TERM] as? String to attribute).right()
            }
            is List<*> -> {
                attribute as CompactedAttributeInstances
                attribute.associateBy { it[NGSILD_DATASET_ID_TERM] as? String }.right()
            }
            else -> {
                RevalidationFailedWarning(
                    "The received payload is invalid. Attribute is nor List nor a Map : $attribute"
                ).left()
            }
        }

    private fun CompactedAttributeInstance.isBefore(
        attr: CompactedAttributeInstance,
        property: String
    ): Boolean = (
        (this[property] as? String)?.isDateTime() == true &&
            (attr[property] as? String)?.isDateTime() == true &&
            ZonedDateTime.parse(this[property] as String) < ZonedDateTime.parse(attr[property] as String)
        )
}
