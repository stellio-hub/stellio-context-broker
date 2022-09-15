package com.egm.stellio.search.service

import arrow.core.Either
import arrow.core.continuations.either
import arrow.core.getOrElse
import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.model.*
import com.egm.stellio.search.util.addSysAttrs
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonUtils.deserializeExpandedPayload
import com.egm.stellio.shared.util.entityOrAttrsNotFoundMessage
import org.springframework.stereotype.Service
import java.net.URI

@Service
class QueryService(
    private val entityPayloadService: EntityPayloadService,
    private val attributeInstanceService: AttributeInstanceService,
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val temporalEntityService: TemporalEntityService
) {
    suspend fun queryEntity(
        entityId: URI,
        contexts: List<String>,
        withSysAttrs: Boolean = false
    ): Either<APIException, JsonLdEntity> =
        either {
            val entityPayload = entityPayloadService.retrieve(entityId).bind()
            val temporalEntityAttributes = temporalEntityAttributeService.getForEntity(entityId, emptySet())

            buildJsonLdEntity(temporalEntityAttributes, entityPayload, withSysAttrs, contexts)
        }

    suspend fun queryEntities(
        queryParams: QueryParams,
        accessRightFilter: () -> String?
    ): Either<APIException, Pair<List<JsonLdEntity>, Int>> =
        either {
            val temporalEntityAttributes = temporalEntityAttributeService.getForEntities(
                queryParams,
                accessRightFilter
            )
            if (temporalEntityAttributes.isEmpty())
                return@either Pair<List<JsonLdEntity>, Int>(emptyList(), 0)

            val count = temporalEntityAttributeService.getCountForEntities(
                queryParams,
                accessRightFilter
            ).bind()
            val entitesPayloads = entityPayloadService.retrieve(
                temporalEntityAttributes.map { it.entityId }
            )

            val jsonLdEntities = temporalEntityAttributes
                .groupBy { it.entityId }
                .mapValues { (entityId, teas) ->
                    buildJsonLdEntity(
                        teas,
                        entitesPayloads.find { it.entityId == entityId }!!,
                        queryParams.includeSysAttrs,
                        listOf(queryParams.context)
                    )
                }.values

            Pair(jsonLdEntities.toList(), count).right().bind()
        }

    suspend fun queryTemporalEntity(
        entityId: URI,
        temporalQuery: TemporalQuery,
        withTemporalValues: Boolean,
        withAudit: Boolean,
        contextLink: String
    ): Either<APIException, CompactedJsonLdEntity> =
        either {
            val temporalEntityAttributes = temporalEntityAttributeService.getForEntity(
                entityId,
                temporalQuery.expandedAttrs
            ).let {
                if (it.isEmpty())
                    ResourceNotFoundException(
                        entityOrAttrsNotFoundMessage(entityId.toString(), temporalQuery.expandedAttrs)
                    ).left()
                else it.right()
            }.bind()

            val entityPayload = entityPayloadService.retrieve(entityId).bind()
            val temporalEntityAttributesWithMatchingInstances =
                searchInstancesForTemporalEntityAttributes(temporalEntityAttributes, temporalQuery, withTemporalValues)

            val temporalEntityAttributesWithInstances =
                fillWithTEAWithoutInstances(temporalEntityAttributes, temporalEntityAttributesWithMatchingInstances)

            temporalEntityService.buildTemporalEntity(
                entityPayload,
                temporalEntityAttributesWithInstances,
                temporalQuery,
                listOf(contextLink),
                withTemporalValues,
                withAudit
            )
        }

    suspend fun queryTemporalEntities(
        temporalEntitiesQuery: TemporalEntitiesQuery,
        accessRightFilter: () -> String?
    ): Either<APIException, Pair<List<CompactedJsonLdEntity>, Int>> =
        either {
            val temporalEntityAttributes = temporalEntityAttributeService.getForEntities(
                temporalEntitiesQuery.queryParams,
                accessRightFilter
            )

            val temporalEntityAttributesWithMatchingInstances =
                searchInstancesForTemporalEntityAttributes(
                    temporalEntityAttributes,
                    temporalEntitiesQuery.temporalQuery,
                    temporalEntitiesQuery.withTemporalValues
                )

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

            val count = temporalEntityAttributeService.getCountForEntities(
                temporalEntitiesQuery.queryParams,
                accessRightFilter
            ).getOrElse { 0 }

            Pair(
                temporalEntityService.buildTemporalEntities(
                    attributeInstancesPerEntityAndAttribute,
                    temporalEntitiesQuery.temporalQuery,
                    listOf(temporalEntitiesQuery.queryParams.context),
                    temporalEntitiesQuery.withTemporalValues,
                    temporalEntitiesQuery.withAudit
                ),
                count
            )
        }

    private suspend fun searchInstancesForTemporalEntityAttributes(
        temporalEntityAttributes: List<TemporalEntityAttribute>,
        temporalQuery: TemporalQuery,
        withTemporalValues: Boolean
    ): Map<TemporalEntityAttribute, List<AttributeInstanceResult>> =
        // split the group according to attribute type (measure or any) as this currently triggers 2 different queries
        // then do one search for each type of attribute (fewer queries for improved performance)
        temporalEntityAttributes
            .groupBy {
                it.attributeValueType
            }.mapValues {
                attributeInstanceService.search(temporalQuery, it.value, withTemporalValues)
            }
            .values
            .flatten()
            .groupBy { attributeInstanceResult ->
                // group them by temporal entity attribute
                temporalEntityAttributes.find { tea ->
                    tea.id == attributeInstanceResult.temporalEntityAttribute
                }!!
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

    private fun buildJsonLdEntity(
        temporalEntityAttributes: List<TemporalEntityAttribute>,
        entityPayload: EntityPayload,
        withSysAttrs: Boolean,
        contexts: List<String>
    ): JsonLdEntity {
        val expandedAttributes = temporalEntityAttributes
            .groupBy {
                it.attributeName
            }
            .mapValues { teas ->
                teas.value.map { tea ->
                    tea.payload.deserializeExpandedPayload()
                        .addSysAttrs(withSysAttrs, tea.createdAt, tea.modifiedAt)
                }
            }
            .plus(entityPayload.serializeProperties(withSysAttrs))

        return JsonLdEntity(expandedAttributes, contexts)
    }
}
