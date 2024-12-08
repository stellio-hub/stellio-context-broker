package com.egm.stellio.search.temporal.service

import arrow.core.Either
import arrow.core.Either.Left
import arrow.core.Either.Right
import arrow.core.raise.either
import arrow.core.toOption
import com.egm.stellio.search.authorization.service.AuthorizationService
import com.egm.stellio.search.entity.service.EntityQueryService
import com.egm.stellio.search.entity.service.EntityService
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.ExpandedAttribute
import com.egm.stellio.shared.model.ExpandedAttributes
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.model.addCoreMembers
import com.egm.stellio.shared.model.getMemberValueAsDateTime
import com.egm.stellio.shared.model.toNgsiLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.ngsiLdDateTime
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
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
        entityQueryService.getEntityState(entityId).let {
            when (it) {
                is Left -> {
                    createTemporalEntity(
                        entityId,
                        jsonLdTemporalEntity,
                        jsonLdTemporalEntity.getAttributes().sorted(),
                        sub
                    ).bind()
                    CreateOrUpdateResult.CREATED
                }
                is Right -> {
                    upsertTemporalEntity(
                        entityId,
                        jsonLdTemporalEntity,
                        jsonLdTemporalEntity.getAttributes().sorted(),
                        it.value.second != null,
                        sub
                    ).bind()
                    CreateOrUpdateResult.UPSERTED
                }
            }
        }
    }

    internal suspend fun createTemporalEntity(
        entityId: URI,
        jsonLdTemporalEntity: ExpandedEntity,
        sortedJsonLdInstances: ExpandedAttributes,
        sub: Sub? = null
    ): Either<APIException, Unit> = either {
        authorizationService.userCanCreateEntities(sub.toOption()).bind()

        val (expandedEntity, ngsiLdEntity) = parseExpandedInstances(sortedJsonLdInstances, jsonLdTemporalEntity).bind()
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
        jsonLdTemporalEntity: ExpandedEntity,
        sortedJsonLdInstances: ExpandedAttributes,
        isDeleted: Boolean,
        sub: Sub? = null
    ): Either<APIException, Unit> = either {
        authorizationService.userCanUpdateEntity(entityId, sub.toOption()).bind()
        if (isDeleted) {
            val (expandedEntity, ngsiLdEntity) =
                parseExpandedInstances(sortedJsonLdInstances, jsonLdTemporalEntity).bind()
            entityService.createEntityPayload(ngsiLdEntity, expandedEntity, ngsiLdDateTime(), sub).bind()
        }
        entityService.upsertAttributes(
            entityId,
            sortedJsonLdInstances,
            sub
        ).bind()
    }

    private suspend fun parseExpandedInstances(
        sortedJsonLdInstances: ExpandedAttributes,
        jsonLdTemporalEntity: ExpandedEntity
    ): Either<APIException, Pair<ExpandedEntity, NgsiLdEntity>> = either {
        // create a view of the entity containing only the most recent instance of each attribute
        val expandedEntity = ExpandedEntity(
            sortedJsonLdInstances
                .keepFirstInstances()
                .addCoreMembers(jsonLdTemporalEntity.id, jsonLdTemporalEntity.types)
        )
        val ngsiLdEntity = expandedEntity.toNgsiLdEntity().bind()

        Pair(expandedEntity, ngsiLdEntity)
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

    @Transactional
    suspend fun deleteEntity(
        entityId: URI,
        sub: Sub? = null
    ): Either<APIException, Unit> = either {
        entityService.permanentlyDeleteEntity(entityId, sub).bind()
    }

    @Transactional
    suspend fun deleteAttribute(
        entityId: URI,
        attributeName: ExpandedTerm,
        datasetId: URI?,
        deleteAll: Boolean = false,
        sub: Sub? = null
    ): Either<APIException, Unit> = either {
        entityService.permanentlyDeleteAttribute(entityId, attributeName, datasetId, deleteAll, sub).bind()
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
