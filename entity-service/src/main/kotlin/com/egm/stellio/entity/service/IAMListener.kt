package com.egm.stellio.entity.service

import com.egm.stellio.shared.model.AuthorizationEvent
import com.egm.stellio.shared.model.EventType
import com.egm.stellio.shared.model.parseToNgsiLdAttributes
import com.egm.stellio.shared.model.toNgsiLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdKey
import com.egm.stellio.shared.util.JsonUtils.parseAuthorizationEvent
import com.egm.stellio.shared.util.toUri
import org.slf4j.LoggerFactory
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.stereotype.Component

@Component
@EnableBinding(IAMEventSink::class)
class IAMListener(
    private val entityService: EntityService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @StreamListener("cim.iam")
    fun processMessage(content: String) {
        val authorizationEvent: AuthorizationEvent = parseAuthorizationEvent(content)

        when (authorizationEvent.operationType) {
            EventType.CREATE -> create(authorizationEvent)
            EventType.APPEND -> append(authorizationEvent)
            EventType.UPDATE -> update(authorizationEvent)
            EventType.DELETE -> delete(authorizationEvent)
            else -> logger.info("Authorization event ${authorizationEvent.operationType} not handled.")
        }
    }

    private fun create(authorizationEvent: AuthorizationEvent) {
        val ngsiLdEntity = expandJsonLdEntity(
            authorizationEvent.payload!!,
            authorizationEvent.context!!
        ).toNgsiLdEntity()

        entityService.createEntity(ngsiLdEntity)
    }

    private fun append(authorizationEvent: AuthorizationEvent) {
        val expandedJsonLdFragment =
            expandJsonLdFragment(authorizationEvent.payload!!, authorizationEvent.context!!)

        entityService.appendEntityAttributes(
            authorizationEvent.entityId,
            parseToNgsiLdAttributes(expandedJsonLdFragment),
            false
        )
    }

    private fun update(authorizationEvent: AuthorizationEvent) {
        val expandedJsonLdFragment =
            expandJsonLdFragment(authorizationEvent.payload!!, authorizationEvent.context!!)

        entityService.updateEntityAttributes(
            authorizationEvent.entityId,
            parseToNgsiLdAttributes(expandedJsonLdFragment)
        )
    }

    private fun delete(authorizationEvent: AuthorizationEvent) {
        if (authorizationEvent.attributeName != null) {
            entityService.deleteEntityAttributeInstance(
                authorizationEvent.entityId,
                expandJsonLdKey(authorizationEvent.attributeName!!, authorizationEvent.context!!)!!,
                authorizationEvent.datasetId!!.toUri()
            )
        } else {
            entityService.deleteEntity(authorizationEvent.entityId)
        }
    }
}
