package com.egm.stellio.search.csr.service

import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_COMPACTED_ENTITY_CORE_MEMBERS
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_TERM
import com.egm.stellio.shared.util.JsonLdUtils.logger
import com.egm.stellio.shared.util.isDate
import org.json.XMLTokener.entity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitExchange
import sun.jvm.hotspot.oops.CellTypeState.value
import java.net.URI
import java.time.LocalDateTime
import kotlin.random.Random.Default.nextBoolean

typealias SingleAttribute = Map<String, Any> // todo maybe use the actual attribute type
typealias CompactedAttribute = List<SingleAttribute>

object ContextSourceUtils {

    suspend fun call(
        httpHeaders: HttpHeaders,
        csr: ContextSourceRegistration,
        path: URI,
        body: String,
        method: HttpMethod
    ): Any {
        val uri = csr.endpoint
        val request =
            WebClient.create("$uri/$path")
                .method(method)
                .headers { newHeader -> httpHeaders.entries.forEach { (key, value) -> newHeader[key] = value } }
        val response: Any = request
            .bodyValue(body)
            .awaitExchange { response ->
                logger.info(
                    "The csr request has return a ${response.statusCode()}}"
                )
            }
        return response
    }

    /**
     * Implements 4.5.5 - Multi-Attribute Support
     */
    fun mergeEntity(
        entities: List<CompactedEntity>,
        auxiliaryEntities: List<CompactedEntity> = listOf()
    ): CompactedEntity? {
        val initialEntity = entities.getOrNull(0) ?: auxiliaryEntities.getOrNull(0)
        if ((entities.size + auxiliaryEntities.size) <= 1) {
            return initialEntity
        }
        val mergedEntity = initialEntity!!.toMutableMap()
        entities.forEach {
                entity ->
            entity.entries.forEach {
                    (key, value) ->
                when {
                    JSONLD_COMPACTED_ENTITY_CORE_MEMBERS.contains(key) -> {}
                    !mergedEntity.containsKey(key) -> mergedEntity[key] = value
                    else -> mergedEntity[key] = mergeAttribute(mergedEntity[key]!!, value)
                }
            }
        }
        auxiliaryEntities.forEach { entity ->
            entity.entries.forEach { (key, value) ->
                when {
                    JSONLD_COMPACTED_ENTITY_CORE_MEMBERS.contains(key) -> {}
                    !mergedEntity.containsKey(key) -> mergedEntity[key] = value
                    else -> mergedEntity[key] = mergeAttribute(mergedEntity[key]!!, value, true)
                }
            }
        }

        return mergedEntity
    }

    fun mergeAttribute(
        attribute1: Any,
        attribute2: Any,
        auxiliary: Boolean = false
    ): CompactedAttribute {
        val mergeMap = attributeToDatasetIdMap(attribute1).toMutableMap()
        val attribute2Map = attributeToDatasetIdMap(attribute2)
        attribute2Map.entries.forEach { (datasetId, value) ->
            when {
                mergeMap[datasetId] == null -> mergeMap[datasetId] = value
                auxiliary -> {}
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
            mapOf(attribute[NGSILD_DATASET_ID_TERM] as String to attribute)
        }
        is List<*> -> {
            attribute as CompactedAttribute
            attribute.associate {
                it[NGSILD_DATASET_ID_TERM] as String to it
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
        (this[property] as? String)?.isDate() == true &&
            (attr[property] as? String)?.isDate() == true &&
            LocalDateTime.parse(this[property] as String) < LocalDateTime.parse(attr[property] as String)
        )
}
