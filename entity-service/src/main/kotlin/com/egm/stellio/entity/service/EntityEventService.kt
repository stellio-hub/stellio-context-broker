package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.UpdateOperationResult
import com.egm.stellio.entity.model.UpdateResult
import com.egm.stellio.shared.model.AttributeAppendEvent
import com.egm.stellio.shared.model.AttributeReplaceEvent
import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver
import org.springframework.http.MediaType
import org.springframework.messaging.MessageHeaders
import org.springframework.messaging.support.MessageBuilder
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Component
import java.net.URI

@Component
class EntityEventService(
    private val resolver: BinderAwareChannelResolver
) {
    private val mapper: ObjectMapper =
        jacksonObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

    @Async
    fun publishEntityEvent(event: EntityEvent, channelSuffix: String): java.lang.Boolean {
        return resolver.resolveDestination(entityChannelName(channelSuffix))
            .send(
                MessageBuilder.createMessage(
                    mapper.writeValueAsString(event),
                    MessageHeaders(mapOf(MessageHeaders.ID to event.entityId))
                )
            ) as java.lang.Boolean
    }

    private fun entityChannelName(channelSuffix: String) =
        "cim.entity.$channelSuffix"

    fun publishAppendEntityAttributesEvents(
        entityId: URI,
        jsonLdAttributes: Map<String, Any>,
        appendResult: UpdateResult,
        updatedEntity: JsonLdEntity,
        contexts: List<String>
    ) {
        appendResult.updated.forEach { updatedDetails ->
            if (updatedDetails.updateOperationResult == UpdateOperationResult.APPENDED)
                publishEntityEvent(
                    AttributeAppendEvent(
                        entityId,
                        compactTerm(updatedDetails.attributeName, contexts),
                        updatedDetails.datasetId,
                        JsonLdUtils.compactAndStringifyFragment(
                            updatedDetails.attributeName,
                            jsonLdAttributes[updatedDetails.attributeName]!!,
                            contexts
                        ),
                        JsonLdUtils.compactAndSerialize(updatedEntity, contexts, MediaType.APPLICATION_JSON),
                        contexts
                    ),
                    compactTerm(updatedEntity.type, contexts)
                )
            else
                publishEntityEvent(
                    AttributeReplaceEvent(
                        entityId,
                        compactTerm(updatedDetails.attributeName, contexts),
                        updatedDetails.datasetId,
                        JsonLdUtils.compactAndStringifyFragment(
                            updatedDetails.attributeName,
                            jsonLdAttributes[updatedDetails.attributeName]!!,
                            contexts
                        ),
                        JsonLdUtils.compactAndSerialize(updatedEntity, contexts, MediaType.APPLICATION_JSON),
                        contexts
                    ),
                    compactTerm(updatedEntity.type, contexts)
                )
        }
    }
}
