package com.egm.stellio.search.csr.service

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.search.csr.model.Mode
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID_TERM
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SCOPE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.logger
import com.egm.stellio.shared.util.isDateTime
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.util.MultiValueMap
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBody
import org.springframework.web.reactive.function.client.awaitExchange
import java.net.URI
import java.time.ZonedDateTime
import kotlin.random.Random.Default.nextBoolean

typealias CompactedEntityWithMode = Pair<CompactedEntity, Mode>

object ContextSourceUtils {

    suspend fun call(
        httpHeaders: HttpHeaders,
        csr: ContextSourceRegistration,
        method: HttpMethod,
        path: String,
        params: MultiValueMap<String, String>
    ): Either<APIException, CompactedEntity> = either {
        val uri = URI("${csr.endpoint}$path")
        val request = WebClient.create()
            .method(method)
            .uri { uriBuilder ->
                uriBuilder.scheme(uri.scheme)
                    .host(uri.host)
                    .path(uri.path)
                    .queryParams(params)
                    .build()
            }
            .header(HttpHeaders.LINK, httpHeaders.getFirst(HttpHeaders.LINK))
        val (statusCode, response) = request
            .awaitExchange { response -> response.statusCode() to response.awaitBody<CompactedEntity>() }
        return if (statusCode.is2xxSuccessful) {
            logger.debug("Successfully received response from CSR at $uri")
            response.right()
        } else {
            logger.warn("Error contacting CSR at $uri: $response")
            ContextSourceRequestException(
                response.toString(),
                HttpStatus.valueOf(statusCode.value())
            ).left()
        }
    }

    fun mergeEntity(
        localEntity: CompactedEntity?,
        entitiesWithMode: List<CompactedEntityWithMode>
    ): CompactedEntity? {
        if (localEntity == null && entitiesWithMode.isEmpty()) return null

        val mergedEntity = localEntity?.toMutableMap() ?: mutableMapOf()

        entitiesWithMode.sortedBy { (_, mode) -> mode == Mode.AUXILIARY }
            .forEach { (entity, mode) ->
                entity.entries.forEach {
                        (key, value) ->
                    val mergedValue = mergedEntity[key]
                    when {
                        mergedValue == null -> mergedEntity[key] = value
                        key == JSONLD_ID_TERM || key == JSONLD_CONTEXT -> {}
                        key == JSONLD_TYPE_TERM || key == NGSILD_SCOPE_TERM ->
                            mergedEntity[key] = mergeTypeOrScope(mergedValue, value)
                        else -> mergedEntity[key] = mergeAttribute(
                            mergedValue,
                            value,
                            mode == Mode.AUXILIARY
                        )
                    }
                }
            }
        return mergedEntity
    }

    fun mergeTypeOrScope(
        type1: Any, // String || List<String> || Set<String>
        type2: Any
    ) = when {
        type1 is List<*> && type2 is List<*> -> (type1.toSet() + type2.toSet()).toList()
        type1 is List<*> -> (type1.toSet() + type2).toList()
        type2 is List<*> -> (type2.toSet() + type1).toList()
        type1 == type2 -> type1
        else -> listOf(type1, type2)
    }

    /**
     * Implements 4.5.5 - Multi-Attribute Support
     */
    fun mergeAttribute(
        attribute1: Any,
        attribute2: Any,
        isAuxiliary: Boolean = false
    ): Any {
        val mergeMap = attributeToDatasetIdMap(attribute1).toMutableMap()
        val attribute2Map = attributeToDatasetIdMap(attribute2)
        attribute2Map.entries.forEach { (datasetId, value2) ->
            val value1 = mergeMap[datasetId]
            when {
                value1 == null -> mergeMap[datasetId] = value2
                isAuxiliary -> {}
                value1.isBefore(value2, NGSILD_OBSERVED_AT_TERM) -> mergeMap[datasetId] = value2
                value2.isBefore(value1, NGSILD_OBSERVED_AT_TERM) -> {}
                value1.isBefore(value2, NGSILD_MODIFIED_AT_TERM) -> mergeMap[datasetId] = value2
                value2.isBefore(value1, NGSILD_MODIFIED_AT_TERM) -> {}
                nextBoolean() -> mergeMap[datasetId] = value2
                else -> {}
            }
        }
        val values = mergeMap.values.toList()
        return if (values.size == 1) values[0] else values
    }

    private fun attributeToDatasetIdMap(attribute: Any): Map<String?, CompactedAttributeInstance> = when (attribute) {
        is Map<*, *> -> {
            attribute as CompactedAttributeInstance
            mapOf(attribute[NGSILD_DATASET_ID_TERM] as? String to attribute)
        }
        is List<*> -> {
            attribute as CompactedAttributeInstances
            attribute.associateBy { it[NGSILD_DATASET_ID_TERM] as? String }
        }
        else -> throw InternalErrorException(
            "the attribute is nor a list nor a map, check that you have excluded the CORE Members"
        )
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
