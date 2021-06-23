package com.egm.stellio.entity.web

import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.service.EntityService
import com.egm.stellio.shared.model.AttributeAppendEvent
import com.egm.stellio.shared.model.AttributeDeleteEvent
import com.egm.stellio.shared.model.NgsiLdRelationship
import com.egm.stellio.shared.model.NgsiLdRelationshipInstance
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.model.parseToNgsiLdAttributes
import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonUtils
import com.egm.stellio.shared.util.checkAndGetContext
import com.egm.stellio.shared.util.getContextFromLinkHeaderOrDefault
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.shared.web.extractSubjectOrEmpty
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono

@RestController
@RequestMapping("/ngsi-ld/v1/entityAccessControl")
class EntityAccessControlHandler(
    private val entityService: EntityService,
    private val authorizationService: AuthorizationService,
    private val kafkaTemplate: KafkaTemplate<String, String>
) {

    @PostMapping("/{subjectId}/attrs", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun addRightsOnEntities(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable subjectId: String,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> {
        val userId = extractSubjectOrEmpty().awaitFirst()
        val body = requestBody.awaitFirst()
        val contexts = checkAndGetContext(httpHeaders, body)
        val jsonLdAttributes = JsonLdUtils.expandJsonLdFragment(body, contexts)
        val ngsiLdAttributes = parseToNgsiLdAttributes(jsonLdAttributes)

        val (authorizedInstances, unauthorizedInstances) = ngsiLdAttributes
            .map { ngsiLdAttribute ->
                (ngsiLdAttribute.getAttributeInstances() as List<NgsiLdRelationshipInstance>)
                    .map { Pair(ngsiLdAttribute, it) }
            }.flatten()
            .partition {
                // we don't have any sub-relationships here, so let's just take the first
                val targetEntityId = it.second.getLinkedEntitiesIds().first()
                authorizationService.userIsAdminOfEntity(targetEntityId, userId)
            }

        authorizedInstances.forEach {
            val ngsiLdRelationship = it.first as NgsiLdRelationship
            entityService.appendEntityRelationship(
                subjectId.toUri(),
                ngsiLdRelationship,
                it.second,
                false
            )

            val operationPayload = mapOf(
                ngsiLdRelationship.compactName to mapOf(
                    "type" to "Relationship",
                    "object" to it.second.objectId,
                    "datasetId" to it.second.datasetId
                )
            )
            val attributeAppendEvent = AttributeAppendEvent(
                entityId = subjectId.toUri(),
                attributeName = ngsiLdRelationship.compactName,
                datasetId = it.second.datasetId,
                operationPayload = JsonUtils.serializeObject(operationPayload),
                updatedEntity = "",
                contexts = contexts
            )
            kafkaTemplate.send("cim.iam.rights", subjectId, JsonUtils.serializeObject(attributeAppendEvent))
        }

        return if (unauthorizedInstances.isEmpty())
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        else {
            val unauthorizedEntities =
                unauthorizedInstances.map { it.second.objectId }
                    .joinToString(",") { "\"$it\"" }
            ResponseEntity.status(HttpStatus.MULTI_STATUS).body(
                """
                    {
                        "unauthorized entities": [$unauthorizedEntities]
                    }
                """.trimIndent()
            )
        }
    }

    @DeleteMapping("/{subjectId}/attrs/{entityId}")
    suspend fun removeRightsOnEntity(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable subjectId: String,
        @PathVariable entityId: String
    ): ResponseEntity<*> {
        val userId = extractSubjectOrEmpty().awaitFirst()
        val contexts = listOf(getContextFromLinkHeaderOrDefault(httpHeaders))

        if (!authorizationService.userIsAdminOfEntity(entityId.toUri(), userId))
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body("User is not authorized to manage rights on entity $entityId")

        val removeResult = authorizationService.removeUserRightsOnEntity(entityId.toUri(), subjectId.toUri())

        if (removeResult == 1) {
            val attributeAppendEvent = AttributeDeleteEvent(
                entityId = subjectId.toUri(),
                attributeName = entityId,
                datasetId = null,
                updatedEntity = "",
                contexts = contexts
            )
            kafkaTemplate.send("cim.iam.rights", subjectId, JsonUtils.serializeObject(attributeAppendEvent))
        }

        return if (removeResult == 1)
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        else
            throw ResourceNotFoundException("Subject $subjectId has no right on entity $entityId")
    }
}
