package com.egm.stellio.entity.web

import com.egm.stellio.entity.service.EntityTypeService
import com.egm.stellio.entity.util.decode
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdKey
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
    private val entityTypeService: EntityTypeService
) {

    /**
     * Implements 6.26 - Retrieve Available Entity Type Information
     */
    @GetMapping("/{type}", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getByType(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable type: String
    ): ResponseEntity<*> {
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
        val expandedType = expandJsonLdKey(type.decode(), contextLink)!!

        val entityTypeInformation = entityTypeService.getEntityTypeInformation(expandedType)
            ?: throw ResourceNotFoundException("No entities found for type $expandedType")

        return ResponseEntity.status(HttpStatus.OK).body(JsonUtils.serializeObject(entityTypeInformation))
    }
}
