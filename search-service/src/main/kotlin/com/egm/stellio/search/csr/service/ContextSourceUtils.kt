package com.egm.stellio.search.csr.service

import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_COMPACTED_ENTITY_CORE_MEMBERS
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_TERM
import com.egm.stellio.shared.util.JsonLdUtils.logger
import com.egm.stellio.shared.util.isDate
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBody
import org.springframework.web.reactive.function.client.awaitExchange
import java.time.LocalDateTime
import kotlin.random.Random.Default.nextBoolean

typealias SingleAttribute = Map<String, Any> // todo maybe use the actual attribute type
typealias CompactedAttribute = List<SingleAttribute>
typealias CompactedEntityWithIsAuxiliary = Pair<CompactedEntity, Boolean>

object ContextSourceUtils {

    suspend fun call(
        httpHeaders: HttpHeaders,
        csr: ContextSourceRegistration,
        method: HttpMethod,
        path: String,
        body: String? = null
    ): CompactedEntity? {
        val uri = "${csr.endpoint}$path"
        val request = WebClient.create(uri)
            .method(method)
            .headers { newHeader -> "Link" to httpHeaders["Link"] }
        body?.let { request.bodyValue(it) }
        val (statusCode, response) = request
            .awaitExchange { response -> response.statusCode() to response.awaitBody<CompactedEntity>() }
        return if (statusCode.is2xxSuccessful) {
            logger.info("Successfully received Informations from CSR at : $uri")

            response
        } else {
            logger.info("Error contacting CSR at : $uri")
            logger.info("Error contacting CSR at : $response")
            null
        }
    }

    fun mergeEntity(
        localEntity: CompactedEntity?,
        pairsOfEntitiyWithISAuxiliary: List<CompactedEntityWithIsAuxiliary>
    ): CompactedEntity? {
        if (localEntity == null && pairsOfEntitiyWithISAuxiliary.isEmpty()) return null

        val mergedEntity = localEntity?.toMutableMap() ?: mutableMapOf()

        pairsOfEntitiyWithISAuxiliary.forEach {
                entity ->
            entity.first.entries.forEach {
                    (key, value) ->
                when {
                    !mergedEntity.containsKey(key) -> mergedEntity[key] = value
                    JSONLD_COMPACTED_ENTITY_CORE_MEMBERS.contains(key) -> {}
                    else -> mergedEntity[key] = mergeAttribute(mergedEntity[key]!!, value, entity.second)
                }
            }
        }
        return mergedEntity
    }

    /**
     * Implements 4.5.5 - Multi-Attribute Support
     */
    private fun mergeAttribute(
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
        (this[property] as? String)?.isDate() == true &&
            (attr[property] as? String)?.isDate() == true &&
            LocalDateTime.parse(this[property] as String) < LocalDateTime.parse(attr[property] as String)
        )
}
