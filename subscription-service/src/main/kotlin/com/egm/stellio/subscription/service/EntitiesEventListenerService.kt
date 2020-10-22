package com.egm.stellio.subscription.service

import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class EntitiesEventListenerService(
    private val notificationService: NotificationService
) {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val mapper =
        jacksonObjectMapper().findAndRegisterModules().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)

    // using @KafkaListener instead of @StreamListener, couldn't find way to specify topic patterns with @StreamListener
    @KafkaListener(topicPattern = "cim.entity.*", groupId = "context_subscription")
    fun processMessage(content: String) {
        val entityEvent = parseEntitiesEvent(content)
        val entity = getEntityFromEvent(entityEvent)
        entity?.let {
            try {
                val updatedFragment = JsonLdUtils.parseJsonLdFragment(
                    getEventPayload(entityEvent)
                        ?: throw UnsupportedEventTypeException("Received unsupported event type")
                )
                val parsedEntity = JsonLdUtils.expandJsonLdEntity(it)
                notificationService.notifyMatchingSubscribers(it, parsedEntity.toNgsiLdEntity(), updatedFragment.keys)
                    .subscribe {
                        val succeeded = it.filter { it.third }.size
                        val failed = it.filter { !it.third }.size
                        logger.debug("Notified ${it.size} subscribers (success : $succeeded / failure : $failed)")
                    }
            } catch (e: UnsupportedEventTypeException) {
                logger.error(e.message)
            } catch (e: BadRequestDataException) {
                logger.error("Received a non-parseable entity : $content", e)
            } catch (e: InvalidRequestException) {
                logger.error("Received a non-parseable entity : $content", e)
            }
        }
    }

    private fun getEntityFromEvent(entityEvent: EntitiesEvent): String? {
        return when (entityEvent) {
            is EntityCreateEvent -> {
                entityEvent.operationPayload
            }
            is EntityDeleteEvent -> {
                logger.warn("Entity delete operation is not yet implemented")
                null
            }
            is AttributeAppendEvent -> {
                entityEvent.updatedEntity
            }
            is AttributeReplaceEvent -> {
                entityEvent.updatedEntity
            }
            is AttributeUpdateEvent -> {
                entityEvent.updatedEntity
            }
            is AttributeDeleteEvent -> {
                logger.warn("Attribute delete operation is not yet implemented")
                null
            }
            else -> {
                logger.warn("Unsupported event type")
                null
            }
        }
    }

    private fun getEventPayload(entityEvent: EntitiesEvent): String? {
        return when (entityEvent) {
            is EntityCreateEvent -> {
                entityEvent.operationPayload
            }
            is AttributeAppendEvent -> {
                entityEvent.updatedEntity
            }
            is AttributeReplaceEvent -> {
                entityEvent.updatedEntity
            }
            is AttributeUpdateEvent -> {
                entityEvent.updatedEntity
            }
            else -> {
                logger.warn("Unsupported event type")
                null
            }
        }
    }

    fun parseEntitiesEvent(input: String): EntitiesEvent =
        mapper.readValue(input, EntitiesEvent::class.java)
}
