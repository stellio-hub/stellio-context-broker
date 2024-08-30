package com.egm.stellio.search.temporal.service

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.toOption
import com.egm.stellio.search.authorization.service.AuthorizationService
import com.egm.stellio.search.entity.service.EntityQueryService
import com.egm.stellio.search.entity.service.EntityService
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.Sub
import org.springframework.stereotype.Service
import java.net.URI

@Service
class TemporalService(
    private val entityService: EntityService,
    private val entityQueryService: EntityQueryService,
    private val attributeInstanceService: AttributeInstanceService,
    private val authorizationService: AuthorizationService,
) {

    enum class CreateOrUpdateResult { CREATED, UPSERTED }

    suspend fun createOrUpdateTemporalEntity(
        entityId: URI,
        jsonLdTemporalEntity: ExpandedEntity,
        sub: Sub? = null
    ): Either<APIException, CreateOrUpdateResult> = either {
        val entityDoesNotExist = entityQueryService.checkEntityExistence(entityId, true).isRight()

        if (entityDoesNotExist) {
            createTemporalEntity(
                entityId,
                jsonLdTemporalEntity,
                jsonLdTemporalEntity.getAttributes().sorted(),
                sub
            ).bind()

            CreateOrUpdateResult.CREATED
        } else {
            upsertTemporalEntity(
                entityId,
                jsonLdTemporalEntity.getAttributes().sorted(),
                sub
            ).bind()

            CreateOrUpdateResult.UPSERTED
        }
    }

    internal suspend fun createTemporalEntity(
        entityId: URI,
        jsonLdTemporalEntity: ExpandedEntity,
        sortedJsonLdInstances: ExpandedAttributes,
        sub: Sub? = null
    ): Either<APIException, Unit> = either {
        authorizationService.userCanCreateEntities(sub.toOption()).bind()

        // create a view of the entity containing only the most recent instance of each attribute
        val expandedEntity = ExpandedEntity(
            sortedJsonLdInstances
                .keepFirstInstances()
                .addCoreMembers(jsonLdTemporalEntity.id, jsonLdTemporalEntity.types)
        )
        val ngsiLdEntity = expandedEntity.toNgsiLdEntity().bind()

        entityService.createEntity(ngsiLdEntity, expandedEntity, sub).bind()
        entityService.upsertAttributes(
            entityId,
            sortedJsonLdInstances.removeFirstInstances(),
            sub
        ).bind()
        authorizationService.createOwnerRight(entityId, sub.toOption()).bind()
    }

    internal suspend fun upsertTemporalEntity(
        entityId: URI,
        sortedJsonLdInstances: ExpandedAttributes,
        sub: Sub? = null
    ): Either<APIException, Unit> = either {
        authorizationService.userCanUpdateEntity(entityId, sub.toOption()).bind()
        entityService.upsertAttributes(
            entityId,
            sortedJsonLdInstances,
            sub
        ).bind()
    }

    private fun ExpandedAttributes.keepFirstInstances(): ExpandedAttributes =
        this.mapValues { listOf(it.value.first()) }

    private fun ExpandedAttributes.removeFirstInstances(): ExpandedAttributes =
        this.mapValues {
            it.value.drop(1)
        }

    private fun ExpandedAttributes.sorted(): ExpandedAttributes =
        this.mapValues {
            it.value.sortedByDescending { expandedAttributePayloadEntry ->
                expandedAttributePayloadEntry.getMemberValueAsDateTime(NGSILD_OBSERVED_AT_PROPERTY)
            }
        }

    suspend fun upsertAttributes(
        entityId: URI,
        jsonLdInstances: ExpandedAttributes,
        sub: Sub? = null
    ): Either<APIException, Unit> = either {
        entityQueryService.checkEntityExistence(entityId).bind()
        authorizationService.userCanUpdateEntity(entityId, sub.toOption()).bind()

        entityService.upsertAttributes(
            entityId,
            jsonLdInstances.sorted(),
            sub
        ).bind()
    }

    suspend fun modifyAttributeInstance(
        entityId: URI,
        instanceId: URI,
        expandedAttribute: ExpandedAttribute,
        sub: Sub? = null
    ): Either<APIException, Unit> = either {
        entityQueryService.checkEntityExistence(entityId).bind()
        authorizationService.userCanUpdateEntity(entityId, sub.toOption()).bind()

        attributeInstanceService.modifyAttributeInstance(
            entityId,
            expandedAttribute.first,
            instanceId,
            expandedAttribute.second
        ).bind()
    }

    suspend fun deleteAttributeInstance(
        entityId: URI,
        attributeName: ExpandedTerm,
        instanceId: URI,
        sub: Sub? = null
    ): Either<APIException, Unit> = either {
        entityQueryService.checkEntityExistence(entityId).bind()
        authorizationService.userCanUpdateEntity(entityId, sub.toOption()).bind()

        attributeInstanceService.deleteInstance(entityId, attributeName, instanceId).bind()
    }
}
