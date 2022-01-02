package com.egm.stellio.entity.web

import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.R_CAN_ADMIN
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.R_CAN_READ
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.R_CAN_WRITE
import com.egm.stellio.entity.service.EntityService
import com.egm.stellio.shared.model.AttributeAppendEvent
import com.egm.stellio.shared.model.EntityCreateEvent
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_EGM_AUTHORIZATION_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.compactAndSerialize
import com.egm.stellio.shared.util.JsonLdUtils.compactFragment
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.extractSubjectOrEmpty
import com.egm.stellio.shared.util.toUri
import kotlinx.coroutines.reactive.awaitFirst
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/admin")
class IAMSynchronizer(
    private val entityService: EntityService,
    private val authorizationService: AuthorizationService,
    private val kafkaTemplate: KafkaTemplate<String, String>,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @PostMapping("/iam/sync")
    suspend fun syncIam(): ResponseEntity<*> {
        val userId = extractSubjectOrEmpty().awaitFirst()
        if (!authorizationService.userIsAdmin(userId))
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body("User is not authorized to sync user referential")

        val authorizationContexts = listOf(NGSILD_EGM_AUTHORIZATION_CONTEXT, NGSILD_CORE_CONTEXT)
        listOf(AuthorizationService.USER_LABEL, AuthorizationService.GROUP_LABEL, AuthorizationService.CLIENT_LABEL)
            .asSequence()
            .map {
                // do a first search without asking for a result in order to get the total count
                val total = entityService.searchEntities(
                    QueryParams(expandedType = it),
                    userId,
                    0,
                    0,
                    NGSILD_EGM_AUTHORIZATION_CONTEXT,
                    false
                ).first
                entityService.searchEntities(
                    QueryParams(expandedType = it),
                    userId,
                    0,
                    total,
                    NGSILD_EGM_AUTHORIZATION_CONTEXT,
                    false
                )
            }
            .map { it.second }
            .flatten()
            .map { jsonLdEntity ->
                val entitiesRightsEvents =
                    generateAttributeAppendEvents(jsonLdEntity, authorizationContexts, R_CAN_ADMIN)
                        .plus(generateAttributeAppendEvents(jsonLdEntity, authorizationContexts, R_CAN_WRITE))
                        .plus(generateAttributeAppendEvents(jsonLdEntity, authorizationContexts, R_CAN_READ))

                val updatedEntity = compactAndSerialize(
                    jsonLdEntity.copy(
                        properties = jsonLdEntity.properties.minus(listOf(R_CAN_ADMIN, R_CAN_WRITE, R_CAN_READ)),
                    ),
                    authorizationContexts,
                    MediaType.APPLICATION_JSON
                )
                val iamEvent = EntityCreateEvent(
                    jsonLdEntity.id.toUri(),
                    jsonLdEntity.type.substringAfterLast("#"),
                    updatedEntity,
                    authorizationContexts
                )
                listOf(iamEvent).plus(entitiesRightsEvents)
            }
            .flatten()
            .toList()
            .forEach {
                val serializedEvent = serializeObject(it)
                logger.debug("Sending event: $serializedEvent")
                kafkaTemplate.send("cim.iam.replay", it.entityId.toString(), serializedEvent)
            }

        return ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }

    private fun generateAttributeAppendEvents(
        jsonLdEntity: JsonLdEntity,
        authorizationContexts: List<String>,
        accessRight: String
    ) = if (jsonLdEntity.properties.containsKey(accessRight)) {
        when (val rCanAdmin = jsonLdEntity.properties[accessRight]) {
            is Map<*, *> ->
                listOf(
                    AttributeAppendEvent(
                        jsonLdEntity.id.toUri(),
                        jsonLdEntity.type.substringAfterLast("#"),
                        accessRight.substringAfterLast("#"),
                        (rCanAdmin[NGSILD_DATASET_ID_PROPERTY] as String).toUri(),
                        true,
                        serializeObject(compactFragment(rCanAdmin as Map<String, Any>, authorizationContexts)),
                        "",
                        authorizationContexts
                    )
                )
            is List<*> ->
                rCanAdmin.map { rCanAdminItem ->
                    rCanAdminItem as Map<String, Any>
                    AttributeAppendEvent(
                        jsonLdEntity.id.toUri(),
                        jsonLdEntity.type.substringAfterLast("#"),
                        accessRight.substringAfterLast("#"),
                        ((rCanAdminItem[NGSILD_DATASET_ID_PROPERTY] as Map<String, Any>)[JSONLD_ID] as String).toUri(),
                        true,
                        serializeObject(compactFragment(rCanAdminItem, authorizationContexts)),
                        "",
                        authorizationContexts
                    )
                }
            else -> {
                logger.warn("Unsupported representation for $accessRight: $rCanAdmin")
                emptyList()
            }
        }
    } else emptyList()
}
