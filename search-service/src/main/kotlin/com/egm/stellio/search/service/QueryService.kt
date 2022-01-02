package com.egm.stellio.search.service

import com.egm.stellio.search.config.ApplicationProperties
import com.egm.stellio.search.model.AttributeInstanceResult
import com.egm.stellio.search.model.TemporalEntitiesQuery
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.search.web.buildTemporalQuery
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.CompactedJsonLdEntity
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.*
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrDefault
import org.springframework.stereotype.Service
import org.springframework.util.MultiValueMap
import java.net.URI
import java.util.*

@Service
class QueryService(
    private val attributeInstanceService: AttributeInstanceService,
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val applicationProperties: ApplicationProperties,
    private val temporalEntityService: TemporalEntityService
) {

    fun parseAndCheckQueryParams(
        queryParams: MultiValueMap<String, String>,
        contextLink: String
    ): TemporalEntitiesQuery {
        val withTemporalValues = hasValueInOptionsParam(
            Optional.ofNullable(queryParams.getFirst("options")), OptionsParamValue.TEMPORAL_VALUES
        )
        val count = queryParams.getFirst(QUERY_PARAM_COUNT)?.toBoolean() ?: false
        val ids = parseRequestParameter(queryParams.getFirst(QUERY_PARAM_ID)).map { it.toUri() }.toSet()
        val types = parseAndExpandRequestParameter(queryParams.getFirst(QUERY_PARAM_TYPE), contextLink)
        val temporalQuery = buildTemporalQuery(queryParams, contextLink)
        val (offset, limit) = extractAndValidatePaginationParameters(
            queryParams,
            applicationProperties.pagination.limitDefault,
            applicationProperties.pagination.limitMax,
            count
        )

        if (types.isEmpty() && temporalQuery.expandedAttrs.isEmpty())
            throw BadRequestDataException("Either type or attrs need to be present in request parameters")

        return TemporalEntitiesQuery(
            ids = ids,
            types = types,
            temporalQuery = temporalQuery,
            withTemporalValues = withTemporalValues,
            limit = limit,
            offset = offset,
            count = count
        )
    }

    suspend fun queryTemporalEntity(
        entityId: URI,
        temporalQuery: TemporalQuery,
        withTemporalValues: Boolean,
        contextLink: String
    ): CompactedJsonLdEntity {
        val temporalEntityAttributes = temporalEntityAttributeService.getForEntity(
            entityId,
            temporalQuery.expandedAttrs
        ).collectList()
            .awaitFirst()
            .ifEmpty {
                throw ResourceNotFoundException(
                    entityOrAttrsNotFoundMessage(entityId.toString(), temporalQuery.expandedAttrs)
                )
            }

        val temporalEntityAttributesWithMatchingInstances =
            searchInstancesForTemporalEntityAttributes(temporalEntityAttributes, temporalQuery, withTemporalValues)

        val temporalEntityAttributesWithInstances =
            fillWithTEAWithoutInstances(temporalEntityAttributes, temporalEntityAttributesWithMatchingInstances)

        return temporalEntityService.buildTemporalEntity(
            entityId,
            temporalEntityAttributesWithInstances,
            temporalQuery,
            listOf(contextLink),
            withTemporalValues
        )
    }

    suspend fun queryTemporalEntities(
        temporalEntitiesQuery: TemporalEntitiesQuery,
        contextLink: String,
        accessRightFilter: () -> String?
    ): List<CompactedJsonLdEntity> {
        val temporalEntityAttributes = temporalEntityAttributeService.getForEntities(
            temporalEntitiesQuery.limit,
            temporalEntitiesQuery.offset,
            temporalEntitiesQuery.ids,
            temporalEntitiesQuery.types,
            temporalEntitiesQuery.temporalQuery.expandedAttrs,
            accessRightFilter
        ).awaitFirstOrDefault(emptyList())

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

        return temporalEntityService.buildTemporalEntities(
            attributeInstancesPerEntityAndAttribute,
            temporalEntitiesQuery.temporalQuery,
            listOf(contextLink),
            temporalEntitiesQuery.withTemporalValues
        )
    }

    private suspend fun searchInstancesForTemporalEntityAttributes(
        temporalEntityAttributes: List<TemporalEntityAttribute>,
        temporalQuery: TemporalQuery,
        withTemporalValues: Boolean
    ): Map<TemporalEntityAttribute, List<AttributeInstanceResult>> =
        // split the group according to attribute type (measure or any) as this currently triggers 2 different queries
        // then do one search for each type of attribute (less queries for improved performance)
        temporalEntityAttributes
            .groupBy {
                it.attributeValueType
            }.mapValues {
                attributeInstanceService.search(temporalQuery, it.value, withTemporalValues).awaitFirst()
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
        // add them in the result set accompanied with an empty list
        return temporalEntityAttributesWithInstances.plus(
            temporalEntityAttributesWithoutInstances.map { it to emptyList() }
        )
    }
}
