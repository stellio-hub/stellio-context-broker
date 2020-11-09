package com.egm.stellio.search.web

import com.egm.stellio.search.model.AttributeInstanceResult
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.model.TemporalValue
import com.egm.stellio.search.service.AttributeInstanceService
import com.egm.stellio.search.service.EntityService
import com.egm.stellio.search.service.TemporalEntityAttributeService
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.parseSampleDataToJsonLd
import com.egm.stellio.shared.util.toUri
import io.mockk.every
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.ZonedDateTime

internal fun injectTemporalValuesForAttributes(entity: JsonLdEntity, attrExpandedNames: List<String>):
    JsonLdEntity {
        attrExpandedNames.forEach {
            val propList = entity.properties[it] as MutableList<MutableMap<String, *>>
            val propHasValuesList =
                propList[0][JsonLdUtils.NGSILD_PROPERTY_VALUES] as MutableList<MutableMap<String, *>>
            val incomingHasValuesMap = propHasValuesList[0] as MutableMap<String, MutableList<*>>
            incomingHasValuesMap["@list"] = mutableListOf(
                TemporalValue(1543.toDouble(), "2020-01-24T13:01:22.066Z"),
                TemporalValue(1600.toDouble(), "2020-01-24T14:01:22.066Z")
            )
        }
        return entity
    }

internal fun mockWithTemporalProperties(
    withTemporalValues: Boolean,
    attrExpandedNames: List<String>,
    temporalEntityAttributeService: TemporalEntityAttributeService,
    attributeInstanceService: AttributeInstanceService,
    entityService: EntityService
) {
    val entityTemporalProperties = attrExpandedNames.map {
        TemporalEntityAttribute(
            entityId = "entityId".toUri(),
            type = "BeeHive",
            attributeName = it,
            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
        )
    }
    val rawEntity = parseSampleDataToJsonLd()
    val entityFileName = if (withTemporalValues)
        "beehive_with_two_temporal_attributes_evolution_temporal_values.jsonld"
    else
        "beehive_with_two_temporal_attributes_evolution.jsonld"

    val entityWith2temporalEvolutions = if (withTemporalValues) {
        val entity = parseSampleDataToJsonLd(entityFileName)
        injectTemporalValuesForAttributes(entity, attrExpandedNames)
    } else {
        parseSampleDataToJsonLd(entityFileName)
    }

    every { temporalEntityAttributeService.getForEntity(any(), any()) } returns Flux.just(
        entityTemporalProperties[0],
        entityTemporalProperties[1]
    )

    val values = listOf(Pair(1543, "2020-01-24T13:01:22.066Z"), Pair(1600, "2020-01-24T14:01:22.066Z"))
    val attInstanceResults = attrExpandedNames.flatMap { attributeName ->
        values.map {
            AttributeInstanceResult(
                attributeName = attributeName,
                value = it.first,
                observedAt = ZonedDateTime.parse(it.second)
            )
        }
    }

    listOf(Pair(0, entityTemporalProperties[0]), Pair(2, entityTemporalProperties[1])).forEach {
        every {
            attributeInstanceService.search(any(), it.second)
        } returns Mono.just(listOf(attInstanceResults[it.first], attInstanceResults[it.first + 1]))
    }

    every { entityService.getEntityById(any(), any()) } returns Mono.just(rawEntity)
    every {
        temporalEntityAttributeService.injectTemporalValues(any(), any(), any())
    } returns entityWith2temporalEvolutions
}
