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
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBody
import org.springframework.web.reactive.function.client.awaitExchange
import java.time.ZonedDateTime
import java.util.stream.Collectors.toSet
import kotlin.random.Random.Default.nextBoolean

typealias SingleAttribute = Map<String, Any> // todo maybe use the actual attribute type
typealias CompactedAttribute = List<SingleAttribute>
typealias CompactedEntityWithMode = Pair<CompactedEntity, Mode>

object ContextSourceUtils {

    suspend fun call(
        httpHeaders: HttpHeaders,
        csr: ContextSourceRegistration,
        method: HttpMethod,
        path: String,
        body: String? = null
    ): Either<APIException, CompactedEntity> = either {
        val uri = "${csr.endpoint}$path"
        val request = WebClient.create(uri)
            .method(method)
            .headers { newHeader -> "Link" to httpHeaders["Link"] }
        body?.let { request.bodyValue(it) }
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
                    when {
                        !mergedEntity.containsKey(key) -> mergedEntity[key] = value
                        key == JSONLD_ID_TERM || key == JSONLD_CONTEXT -> {}
                        key == JSONLD_TYPE_TERM || key == NGSILD_SCOPE_TERM ->
                            mergedEntity[key] = mergeTypeOrScope(mergedEntity[key]!!, value)
                        else -> mergedEntity[key] = mergeAttribute(
                            mergedEntity[key]!!,
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
        type1 is List<*> && type2 is List<*> -> type1.toSet() + type2.toSet()
        type1 is List<*> -> type1.toSet() + type2
        type2 is List<*> -> type2.toSet() + type1
        type1 == type2 -> setOf(type1)
        else -> setOf(type1, type2)
    }.toList()

    /**
     * Implements 4.5.5 - Multi-Attribute Support
     */
    fun mergeAttribute(
        attribute1: Any,
        attribute2: Any,
        isAuxiliary: Boolean = false
    ): CompactedAttribute {
        val mergeMap = attributeToDatasetIdMap(attribute1).toMutableMap()
        val attribute2Map = attributeToDatasetIdMap(attribute2)
        attribute2Map.entries.forEach { (datasetId, value) ->
            when {
                mergeMap[datasetId] == null -> mergeMap[datasetId] = value
                isAuxiliary -> {}
                mergeMap[datasetId]!!.isBefore(value, NGSILD_OBSERVED_AT_TERM) -> mergeMap[datasetId] = value
                value.isBefore(mergeMap[datasetId]!!, NGSILD_OBSERVED_AT_TERM) -> {}
                mergeMap[datasetId]!!.isBefore(value, NGSILD_MODIFIED_AT_TERM) -> mergeMap[datasetId] = value
                value.isBefore(mergeMap[datasetId]!!, NGSILD_MODIFIED_AT_TERM) -> {}
                nextBoolean() -> mergeMap[datasetId] = value
                else -> {}
            }
        }
        return mergeMap.values.toList()
    }

    private fun attributeToDatasetIdMap(attribute: Any): Map<String?, Map<String, Any>> = when (attribute) {
        is Map<*, *> -> {
            attribute as SingleAttribute
            mapOf(attribute[NGSILD_DATASET_ID_TERM] as? String to attribute)
        }
        is List<*> -> {
            attribute as CompactedAttribute
            attribute.associate {
                it[NGSILD_DATASET_ID_TERM] as? String to it
            }
        }
        else -> throw InternalErrorException(
            "the attribute is nor a list nor a map, check that you have excluded the CORE Members"
        )
    }

    private fun SingleAttribute.isBefore(
        attr: SingleAttribute,
        property: String
    ): Boolean = (
        (this[property] as? String)?.isDateTime() == true &&
            (attr[property] as? String)?.isDateTime() == true &&
            ZonedDateTime.parse(this[property] as String) < ZonedDateTime.parse(attr[property] as String)
        )
}
