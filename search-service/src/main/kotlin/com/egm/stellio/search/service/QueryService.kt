package com.egm.stellio.search.service

import arrow.core.Either
import arrow.core.getOrElse
import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.model.AttributeInstanceResult
import com.egm.stellio.search.model.TemporalEntitiesQuery
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.CompactedJsonLdEntity
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.entityOrAttrsNotFoundMessage
import org.springframework.stereotype.Service
import java.net.URI

@Service
class QueryService(
    private val attributeInstanceService: AttributeInstanceService,
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val temporalEntityService: TemporalEntityService
) {
    suspend fun queryTemporalEntity(
        entityId: URI,
        temporalEntitiesQuery: TemporalEntitiesQuery,
        contextLink: String
    ): Either<APIException, CompactedJsonLdEntity> {
        val temporalEntityAttributes = temporalEntityAttributeService.getForEntity(
            entityId,
            temporalEntitiesQuery.queryParams.attrs
        ).ifEmpty {
            return ResourceNotFoundException(
                entityOrAttrsNotFoundMessage(entityId.toString(), temporalEntitiesQuery.queryParams.attrs)
            ).left()
        }

        val temporalEntityAttributesWithMatchingInstances =
            searchInstancesForTemporalEntityAttributes(
                temporalEntityAttributes,
                temporalEntitiesQuery.temporalQuery,
                temporalEntitiesQuery.withTemporalValues
            )

        val temporalEntityAttributesWithInstances =
            fillWithTEAWithoutInstances(temporalEntityAttributes, temporalEntityAttributesWithMatchingInstances)

        return temporalEntityService.buildTemporalEntity(
            entityId,
            temporalEntityAttributesWithInstances,
            temporalEntitiesQuery.temporalQuery,
            listOf(contextLink),
            temporalEntitiesQuery.withTemporalValues,
            temporalEntitiesQuery.withAudit
        ).right()
    }

    suspend fun queryTemporalEntities(
        temporalEntitiesQuery: TemporalEntitiesQuery,
        contextLink: String,
        accessRightFilter: () -> String?
    ): Pair<List<CompactedJsonLdEntity>, Int> {
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
                }
                .mapValues {
                    it.value.toMap()
                }
                .toList()
                // the ordering made when searching matching temporal entity attributes is lost
                // since we are now iterating over the map of TEAs with their instances
                .sortedBy { it.first }

        val count = temporalEntityAttributeService.getCountForEntities(
            temporalEntitiesQuery.queryParams,
            accessRightFilter
        ).getOrElse { 0 }

        return Pair(
            temporalEntityService.buildTemporalEntities(
                attributeInstancesPerEntityAndAttribute,
                temporalEntitiesQuery.temporalQuery,
                listOf(contextLink),
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
}
