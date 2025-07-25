package com.egm.stellio.search.temporal.service

import arrow.core.Either
import arrow.core.getOrElse
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.authorization.service.AuthorizationService
import com.egm.stellio.search.entity.model.Attribute
import com.egm.stellio.search.entity.service.EntityAttributeService
import com.egm.stellio.search.entity.service.EntityQueryService
import com.egm.stellio.search.scope.ScopeService
import com.egm.stellio.search.temporal.model.EntityTemporalResult
import com.egm.stellio.search.temporal.model.SimplifiedAttributeInstanceResult
import com.egm.stellio.search.temporal.model.TemporalEntitiesQuery
import com.egm.stellio.search.temporal.service.TemporalPaginationService.getPaginatedAttributeWithInstancesAndRange
import com.egm.stellio.search.temporal.util.AttributesWithInstances
import com.egm.stellio.search.temporal.util.TemporalEntityBuilder
import com.egm.stellio.search.temporal.util.TemporalRepresentation
import com.egm.stellio.search.temporal.web.Range
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.NGSILD_SCOPE_IRI
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.entityOrAttrsNotFoundMessage
import com.egm.stellio.shared.util.wktToGeoJson
import org.springframework.stereotype.Service
import java.net.URI
import java.time.ZonedDateTime

@Service
class TemporalQueryService(
    private val entityQueryService: EntityQueryService,
    private val scopeService: ScopeService,
    private val attributeInstanceService: AttributeInstanceService,
    private val entityAttributeService: EntityAttributeService,
    private val authorizationService: AuthorizationService,
    private val applicationProperties: ApplicationProperties
) {

    suspend fun queryTemporalEntity(
        entityId: URI,
        temporalEntitiesQuery: TemporalEntitiesQuery
    ): Either<APIException, Pair<ExpandedEntity, Range?>> = either {
        val entity = entityQueryService.retrieve(entityId, false).bind()
        authorizationService.userCanReadEntity(entityId).bind()

        val attrs = temporalEntitiesQuery.entitiesQuery.attrs
        val datasetIds = temporalEntitiesQuery.entitiesQuery.datasetId
        val attributes = entityAttributeService.getForEntity(entityId, attrs, datasetIds, false).let {
            if (it.isEmpty())
                ResourceNotFoundException(
                    entityOrAttrsNotFoundMessage(entityId, temporalEntitiesQuery.entitiesQuery.attrs)
                ).left()
            else it.right()
        }.bind()

        val origin = calculateOldestTimestamp(entityId, temporalEntitiesQuery, attributes)

        val scopeHistory =
            if (attrs.isEmpty() || attrs.contains(NGSILD_SCOPE_IRI))
                scopeService.retrieveHistory(listOf(entityId), temporalEntitiesQuery, origin).bind()
            else emptyList()

        val attributesWithMatchingInstances =
            searchInstancesForAttributes(attributes, temporalEntitiesQuery, origin).bind()

        val (paginatedAttributesWithInstances, range) = getPaginatedAttributeWithInstancesAndRange(
            attributesWithMatchingInstances,
            temporalEntitiesQuery
        )

        TemporalEntityBuilder.buildTemporalEntity(
            EntityTemporalResult(entity, scopeHistory, paginatedAttributesWithInstances),
            temporalEntitiesQuery,
            applicationProperties.contexts.core
        ) to range
    }

    internal suspend fun calculateOldestTimestamp(
        entityId: URI,
        temporalEntitiesQuery: TemporalEntitiesQuery,
        attributes: List<Attribute>
    ): ZonedDateTime? {
        val temporalQuery = temporalEntitiesQuery.temporalQuery

        // time_bucket has a default origin set to 2000-01-03
        // (see https://docs.timescale.com/api/latest/hyperfunctions/time_bucket/)
        // so we force the default origin to:
        // - timeAt if it is provided
        // - the oldest value if not (timeAt is optional if querying a temporal entity by id)

        return if (temporalEntitiesQuery.temporalRepresentation != TemporalRepresentation.AGGREGATED_VALUES)
            null
        else if (temporalQuery.timeAt != null)
            temporalQuery.timeAt
        else {
            val originForAttributes =
                attributeInstanceService.selectOldestDate(temporalQuery, attributes)

            val attrs = temporalEntitiesQuery.entitiesQuery.attrs
            val originForScope =
                if (attrs.isEmpty() || attrs.contains(NGSILD_SCOPE_IRI))
                    scopeService.selectOldestDate(entityId, temporalEntitiesQuery.temporalQuery.timeproperty)
                else null

            when {
                originForAttributes == null -> originForScope
                originForScope == null -> originForAttributes
                else -> minOf(originForAttributes, originForScope)
            }
        }
    }

    suspend fun queryTemporalEntities(
        temporalEntitiesQuery: TemporalEntitiesQuery
    ): Either<APIException, Triple<List<ExpandedEntity>, Int, Range?>> = either {
        val accessRightFilter = authorizationService.computeAccessRightFilter()
        val attrs = temporalEntitiesQuery.entitiesQuery.attrs
        val entitiesIds =
            entityQueryService.queryEntities(temporalEntitiesQuery.entitiesQuery, false, accessRightFilter)
        val count =
            entityQueryService.queryEntitiesCount(temporalEntitiesQuery.entitiesQuery, false, accessRightFilter)
                .getOrElse { 0 }

        // we can have an empty list of entities with a non-zero count (e.g., offset too high)
        if (entitiesIds.isEmpty())
            return@either Triple(emptyList(), count, null)

        val attributes = entityAttributeService.getForEntities(
            entitiesIds,
            temporalEntitiesQuery.entitiesQuery
        )

        val scopesHistory =
            if (attrs.isEmpty() || attrs.contains(NGSILD_SCOPE_IRI))
                scopeService.retrieveHistory(entitiesIds, temporalEntitiesQuery).bind().groupBy { it.entityId }
            else emptyMap()

        val attributesWithMatchingInstances =
            searchInstancesForAttributes(attributes, temporalEntitiesQuery).bind()

        val (paginatedAttributesWithInstances, range) = getPaginatedAttributeWithInstancesAndRange(
            attributesWithMatchingInstances,
            temporalEntitiesQuery
        )

        val attributeInstancesPerEntityAndAttribute =
            paginatedAttributesWithInstances
                .toList()
                .groupBy {
                    // then, group them by entity
                    it.first.entityId
                }.mapKeys {
                    entityQueryService.retrieve(it.key, false).bind()
                }
                .mapValues {
                    it.value.toMap()
                }
                .toList()
                // the ordering made when searching matching attributes is lost
                // since we are now iterating over the map of attributes with their instances
                .sortedBy { it.first.entityId }
                .map {
                    EntityTemporalResult(it.first, scopesHistory[it.first.entityId] ?: emptyList(), it.second)
                }

        Triple(
            TemporalEntityBuilder.buildTemporalEntities(
                attributeInstancesPerEntityAndAttribute,
                temporalEntitiesQuery,
                applicationProperties.contexts.core
            ),
            count,
            range
        )
    }

    private suspend fun searchInstancesForAttributes(
        attributes: List<Attribute>,
        temporalEntitiesQuery: TemporalEntitiesQuery,
        origin: ZonedDateTime? = null
    ): Either<APIException, AttributesWithInstances> = either {
        // split the group according to attribute type as this currently triggers 2 different queries
        // then do one search for each type of attribute (fewer queries for improved performance)
        attributes
            .groupBy {
                it.attributeValueType
            }.mapValues {
                attributeInstanceService.search(temporalEntitiesQuery, it.value, origin).bind()
            }
            .mapValues {
                // when retrieved from DB, values of geo-properties are encoded in WKT and won't be automatically
                // transformed during compaction as it is not done for temporal values, so it is done now
                if (it.key == Attribute.AttributeValueType.GEOMETRY &&
                    temporalEntitiesQuery.temporalRepresentation == TemporalRepresentation.TEMPORAL_VALUES
                ) {
                    it.value.map { attributeInstanceResult ->
                        attributeInstanceResult as SimplifiedAttributeInstanceResult
                        attributeInstanceResult.copy(
                            value = wktToGeoJson(attributeInstanceResult.value as String)
                        )
                    }
                } else it.value
            }
            .values
            .flatten()
            .groupBy { attributeInstanceResult ->
                // group them by temporal entity attribute
                attributes.find { attribute ->
                    attribute.id == attributeInstanceResult.attributeUuid
                }!!
            }
            .mapValues { it.value.sorted() }
    }
}
