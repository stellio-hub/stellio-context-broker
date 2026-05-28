package com.egm.stellio.search.entity.listener

import com.egm.stellio.search.entity.model.TelemetryDataMessage
import com.egm.stellio.search.entity.service.EntityService
import com.egm.stellio.shared.model.JSONLD_TYPE_KW
import com.egm.stellio.shared.model.JSONLD_VALUE_KW
import com.egm.stellio.shared.model.NGSILD_DATASET_ID_IRI
import com.egm.stellio.shared.model.NGSILD_OBSERVED_AT_IRI
import com.egm.stellio.shared.model.NGSILD_PROPERTY_TYPE
import com.egm.stellio.shared.model.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedPropertyValue
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedTemporalValue
import com.egm.stellio.shared.util.JsonUtils
import com.egm.stellio.shared.util.NGSILD_TENANT_HEADER
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class TelemetryListener(
    private val entityService: EntityService
) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val coroutineScope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    @KafkaListener(topics = ["cim.telemetry"], groupId = "search-telemetry")
    fun processTelemetry(content: String) {
        coroutineScope.launch {
            handleTelemetryMessage(content)
        }
    }

    internal suspend fun handleTelemetryMessage(content: String) {
        val message = runCatching {
            JsonUtils.deserializeAs<TelemetryDataMessage>(content)
        }.getOrElse {
            logger.warn("Failed to deserialize telemetry message: ${it.message}")
            return
        }

        val attributeFragment = buildMap {
            put(JSONLD_TYPE_KW, listOf(NGSILD_PROPERTY_TYPE.uri))
            put(NGSILD_PROPERTY_VALUE, listOf(mapOf(JSONLD_VALUE_KW to message.value)))
            put(NGSILD_OBSERVED_AT_IRI, buildNonReifiedTemporalValue(message.observedAt))
            message.datasetId?.let { put(NGSILD_DATASET_ID_IRI, buildNonReifiedPropertyValue(it.toString())) }
        }

        mono {
            entityService.mergeAttribute(
                message.entityId,
                message.attributeName,
                attributeFragment,
                message.observedAt
            )
        }.contextWrite {
            it.put(NGSILD_TENANT_HEADER, message.tenantName)
        }.subscribe { result ->
            result.fold(
                {
                    logger.warn(
                        "Failed to ingest attribute {} for entity {}: {}",
                        message.attributeName,
                        message.entityId,
                        it.message
                    )
                },
                {
                    logger.debug(
                        "Ingested attribute {} for entity {}",
                        message.attributeName,
                        message.entityId
                    )
                }
            )
        }
    }
}
