package com.egm.stellio.search.service

import arrow.core.Either
import arrow.core.getOrElse
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.model.*
import com.egm.stellio.search.util.TemporalEntityBuilder
import com.egm.stellio.search.util.deserializeAsMap
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.entityOrAttrsNotFoundMessage
import com.egm.stellio.shared.util.wktToGeoJson
import org.springframework.stereotype.Service
import java.net.URI

@Service
class QueryService(
    private val entityPayloadService: EntityPayloadService,
    private val attributeInstanceService: AttributeInstanceService,
    private val temporalEntityAttributeService: TemporalEntityAttributeService
) {
    suspend fun queryEntity(
        entityId: URI,
        contexts: List<String>
    ): Either<APIException, JsonLdEntity> =
        either {
            val entityPayload = entityPayloadService.retrieve(entityId).bind()
            toJsonLdEntity(entityPayload, contexts)
        }

    suspend fun queryEntities(
        queryParams: QueryParams,
        accessRightFilter: () -> String?
    ): Either<APIException, Pair<List<JsonLdEntity>, Int>> =
        either {
            val entitiesIds = entityPayloadService.queryEntities(
                queryParams,
                accessRightFilter
            )
            val count = entityPayloadService.queryEntitiesCount(
                queryParams,
                accessRightFilter
            ).bind()

            // we can have an empty list of entities with a non-zero count (e.g., offset too high)
            if (entitiesIds.isEmpty())
                return@either Pair<List<JsonLdEntity>, Int>(emptyList(), count)

            val entitiesPayloads =
                entityPayloadService.retrieve(entitiesIds)
                    .map { toJsonLdEntity(it, listOf(queryParams.context)) }

            Pair(entitiesPayloads, count).right().bind()
        }

    suspend fun queryTemporalEntity(
        entityId: URI,
        temporalEntitiesQuery: TemporalEntitiesQuery,
        contextLink: String
    ): Either<APIException, CompactedJsonLdEntity> =
        either {
            val temporalEntityAttributes = temporalEntityAttributeService.getForEntity(
                entityId,
                temporalEntitiesQuery.queryParams.attrs
            ).let {
                if (it.isEmpty())
                    ResourceNotFoundException(
                        entityOrAttrsNotFoundMessage(entityId.toString(), temporalEntitiesQuery.queryParams.attrs)
                    ).left()
                else it.right()
            }.bind()

            val entityPayload = entityPayloadService.retrieve(entityId).bind()
            val temporalEntityAttributesWithMatchingInstances =
                searchInstancesForTemporalEntityAttributes(temporalEntityAttributes, temporalEntitiesQuery).bind()

            val temporalEntityAttributesWithInstances =
                fillWithTEAWithoutInstances(temporalEntityAttributes, temporalEntityAttributesWithMatchingInstances)

            TemporalEntityBuilder.buildTemporalEntity(
                entityPayload,
                temporalEntityAttributesWithInstances,
                temporalEntitiesQuery,
                listOf(contextLink)
            )
        }

    suspend fun queryTemporalEntities(
        temporalEntitiesQuery: TemporalEntitiesQuery,
        accessRightFilter: () -> String?
    ): Either<APIException, Pair<List<CompactedJsonLdEntity>, Int>> =
        either {
            val entitiesIds = entityPayloadService.queryEntities(temporalEntitiesQuery.queryParams, accessRightFilter)
            val count = entityPayloadService.queryEntitiesCount(
                temporalEntitiesQuery.queryParams,
                accessRightFilter
            ).getOrElse { 0 }

            // we can have an empty list of entities with a non-zero count (e.g., offset too high)
            if (entitiesIds.isEmpty())
                return@either Pair<List<CompactedJsonLdEntity>, Int>(emptyList(), count)

            val temporalEntityAttributes = temporalEntityAttributeService.getForTemporalEntities(
                entitiesIds,
                temporalEntitiesQuery.queryParams
            )

            val temporalEntityAttributesWithMatchingInstances =
                searchInstancesForTemporalEntityAttributes(temporalEntityAttributes, temporalEntitiesQuery).bind()

            val temporalEntityAttributesWithInstances =
                fillWithTEAWithoutInstances(temporalEntityAttributes, temporalEntityAttributesWithMatchingInstances)

            val attributeInstancesPerEntityAndAttribute =
                temporalEntityAttributesWithInstances
                    .toList()
                    .groupBy {
                        // then, group them by entity
                        it.first.entityId
                    }.mapKeys {
                        entityPayloadService.retrieve(it.key).bind()
                    }
                    .mapValues {
                        it.value.toMap()
                    }
                    .toList()
                    // the ordering made when searching matching temporal entity attributes is lost
                    // since we are now iterating over the map of TEAs with their instances
                    .sortedBy { it.first.entityId }

            Pair(
                TemporalEntityBuilder.buildTemporalEntities(
                    attributeInstancesPerEntityAndAttribute,
                    temporalEntitiesQuery,
                    listOf(temporalEntitiesQuery.queryParams.context)
                ),
                count
            )
        }

    private suspend fun searchInstancesForTemporalEntityAttributes(
        temporalEntityAttributes: List<TemporalEntityAttribute>,
        temporalEntitiesQuery: TemporalEntitiesQuery
    ): Either<APIException, Map<TemporalEntityAttribute, List<AttributeInstanceResult>>> = either {
        // split the group according to attribute type as this currently triggers 2 different queries
        // then do one search for each type of attribute (fewer queries for improved performance)
        temporalEntityAttributes
            .groupBy {
                it.attributeValueType
            }.mapValues {
                attributeInstanceService.search(temporalEntitiesQuery, it.value).bind()
            }
            .mapValues {
                // when retrieved from DB, values of geo-properties are encoded in WKT and won't be automatically
                // transformed during compaction as it is not done for temporal values, so it is done now
                if (it.key == TemporalEntityAttribute.AttributeValueType.GEOMETRY &&
                    temporalEntitiesQuery.withTemporalValues
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
                temporalEntityAttributes.find { tea ->
                    tea.id == attributeInstanceResult.temporalEntityAttribute
                }!!
            }
            .mapValues { it.value.sorted() }
    }

    private fun fillWithTEAWithoutInstances(
        temporalEntityAttributes: List<TemporalEntityAttribute>,
        temporalEntityAttributesWithInstances: Map<TemporalEntityAttribute, List<AttributeInstanceResult>>
    ): Map<TemporalEntityAttribute, List<AttributeInstanceResult>> {
        // filter the temporal entity attributes for which there are no attribute instances
        val temporalEntityAttributesWithoutInstances =
            temporalEntityAttributes.filter {
                !temporalEntityAttributesWithInstances.keys.contains(it)
            }
        // add them in the result set accompanied by an empty list
        return temporalEntityAttributesWithInstances.plus(
            temporalEntityAttributesWithoutInstances.map { it to emptyList() }
        )
    }

    private fun toJsonLdEntity(
        entityPayload: EntityPayload,
        contexts: List<String>
    ): JsonLdEntity {
        val deserializedEntity = entityPayload.payload.deserializeAsMap()
        return JsonLdEntity(deserializedEntity, contexts)
    }
}
