package com.egm.stellio.entity.web

import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.service.EntityTypeService
import com.egm.stellio.entity.util.decode
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdKey
import com.egm.stellio.shared.web.extractSubjectOrEmpty
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/ngsi-ld/v1/types")
class EntityTypeHandler(
    private val entityTypeService: EntityTypeService,
    private val authorizationService: AuthorizationService
) {

    /**
     * Implements 6.26 - Retrieve Available Entity Type Information
     */
    @GetMapping("/{type}", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getByType(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable type: String
    ): ResponseEntity<*> {
        val userId = extractSubjectOrEmpty().awaitFirst()
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
        val expandedType = if (type.decode().isExpanded()) type.decode() else expandJsonLdKey(type, contextLink)!!

        val entities = entityTypeService.getEntitiesByType(expandedType)
        if (entities.isEmpty())
            throw ResourceNotFoundException("No entities found for type $expandedType")

        val entitiesUserCanRead =
            authorizationService.filterEntitiesUserCanRead(
                entities.map { it.id.toUri() },
                userId
            ).toListOfString()
        val filteredEntities = entities.filter { entitiesUserCanRead.contains(it.id) }
        if (filteredEntities.isEmpty())
            throw AccessDeniedException("User forbidden read access to entities of type $expandedType")

        val entityTypeInformation = entityTypeService.getEntityTypeInformation(expandedType, filteredEntities)

        return ResponseEntity.status(HttpStatus.OK).body(JsonUtils.serializeObject(entityTypeInformation))
    }
}
