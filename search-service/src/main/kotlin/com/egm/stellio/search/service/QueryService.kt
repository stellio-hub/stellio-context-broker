package com.egm.stellio.search.service

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
    private val temporalEntityService: TemporalEntityService
) {

    fun parseAndCheckQueryParams(queryParams: MultiValueMap<String, String>, contextLink: String): Map<String, Any> {
        val withTemporalValues = hasValueInOptionsParam(
            Optional.ofNullable(queryParams.getFirst("options")), OptionsParamValue.TEMPORAL_VALUES
        )
        val ids = parseRequestParameter(queryParams.getFirst(QUERY_PARAM_ID)).map { it.toUri() }.toSet()
        val types = parseAndExpandRequestParameter(queryParams.getFirst(QUERY_PARAM_TYPE), contextLink)
        val temporalQuery = buildTemporalQuery(queryParams, contextLink)

        if (types.isEmpty() && temporalQuery.expandedAttrs.isEmpty())
            throw BadRequestDataException("Either type or attrs need to be present in request parameters")

        return mapOf(
            "ids" to ids,
            "types" to types,
            "temporalQuery" to temporalQuery,
            "withTemporalValues" to withTemporalValues
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

        // split the group according to attribute type (measure or any) as this currently triggers 2 different queries
        // then do one search for each type of attribute
        val allAttributesInstancesPerAttribute = temporalEntityAttributes
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

        return temporalEntityService.buildTemporalEntity(
            entityId,
            allAttributesInstancesPerAttribute,
            temporalQuery,
            listOf(contextLink),
            withTemporalValues
        )
    }

    suspend fun queryTemporalEntities(
        ids: Set<URI>,
        types: Set<String>,
        temporalQuery: TemporalQuery,
        withTemporalValues: Boolean,
        contextLink: String
    ): List<CompactedJsonLdEntity> {
        val temporalEntityAttributes = temporalEntityAttributeService.getForEntities(
            ids,
            types,
            temporalQuery.expandedAttrs
        ).awaitFirstOrDefault(emptyList())

        // split the group according to attribute type (measure or any) as this currently triggers 2 different queries
        // then do one search for each type of attribute
        val allAttributesInstances =
            temporalEntityAttributes.groupBy {
                it.attributeValueType
            }.mapValues {
                attributeInstanceService.search(temporalQuery, it.value, withTemporalValues).awaitFirst()
            }.values.flatten()

        val attributeInstancesPerEntityAndAttribute =
            allAttributesInstances
                .groupBy {
                    // first, group them by temporal entity attribute
                    temporalEntityAttributes.find { tea ->
                        tea.id == it.temporalEntityAttribute
                    }!!
                }
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
            temporalQuery,
            listOf(contextLink),
            withTemporalValues
        )
    }
}
