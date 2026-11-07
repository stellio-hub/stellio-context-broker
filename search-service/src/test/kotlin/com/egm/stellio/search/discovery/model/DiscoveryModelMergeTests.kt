package com.egm.stellio.search.discovery.model

import com.egm.stellio.shared.util.APIARY_IRI
import com.egm.stellio.shared.util.APIARY_TERM
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.BEEHIVE_TERM
import com.egm.stellio.shared.util.NAME_IRI
import com.egm.stellio.shared.util.NAME_TERM
import com.egm.stellio.shared.util.TEMPERATURE_IRI
import com.egm.stellio.shared.util.TEMPERATURE_TERM
import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test

class DiscoveryModelMergeTests {

    @Test
    fun `merge should return the local list untouched when there is no remote list`() {
        val local = EntityTypeList(typeList = listOf(BEEHIVE_TERM, APIARY_TERM))

        assertSame(local, listOf(local).merge())
    }

    @Test
    fun `merge should return the sorted union of the entity type lists`() {
        val merged = listOf(
            EntityTypeList(typeList = listOf(BEEHIVE_TERM)),
            EntityTypeList(typeList = listOf(BEEHIVE_TERM, APIARY_TERM))
        ).merge()

        assertEquals(listOf(APIARY_TERM, BEEHIVE_TERM), merged.typeList)
    }

    @Test
    fun `merge should return the sorted union of the attribute lists`() {
        val merged = listOf(
            AttributeList(attributeList = listOf(TEMPERATURE_TERM)),
            AttributeList(attributeList = listOf(TEMPERATURE_TERM, NAME_TERM))
        ).merge()

        assertEquals(listOf(NAME_TERM, TEMPERATURE_TERM), merged.attributeList)
    }

    @Test
    fun `merge should group entity types by id and merge their attribute names`() {
        val merged = listOf(
            listOf(
                EntityType(BEEHIVE_IRI.toUri(), typeName = BEEHIVE_TERM, attributeNames = listOf(TEMPERATURE_TERM))
            ),
            listOf(
                EntityType(BEEHIVE_IRI.toUri(), typeName = BEEHIVE_TERM, attributeNames = listOf(NAME_TERM)),
                EntityType(APIARY_IRI.toUri(), typeName = APIARY_TERM, attributeNames = listOf(NAME_TERM))
            )
        ).merge()

        assertEquals(listOf(APIARY_IRI.toUri(), BEEHIVE_IRI.toUri()), merged.map { it.id })
        assertEquals(listOf(NAME_TERM, TEMPERATURE_TERM), merged[1].attributeNames)
    }

    @Test
    fun `merge should group attribute details by id and merge their type names`() {
        val merged = listOf(
            listOf(AttributeDetails(NAME_IRI.toUri(), attributeName = NAME_TERM, typeNames = setOf(BEEHIVE_TERM))),
            listOf(AttributeDetails(NAME_IRI.toUri(), attributeName = NAME_TERM, typeNames = setOf(APIARY_TERM)))
        ).merge()

        assertEquals(1, merged.size)
        assertEquals(setOf(APIARY_TERM, BEEHIVE_TERM), merged[0].typeNames)
    }

    @Test
    fun `merge should sum the entity counts and merge the attribute details of entity type infos`() {
        val merged = listOf(
            entityTypeInfo(2, listOf(AttributeType.Property)),
            entityTypeInfo(3, listOf(AttributeType.JsonProperty, AttributeType.Property))
        ).merge()

        assertEquals(5, merged?.entityCount)
        assertEquals(
            listOf(AttributeType.Property, AttributeType.JsonProperty),
            merged?.attributeDetails?.single()?.attributeTypes
        )
    }

    @Test
    fun `merge should return null when there is no entity type info`() {
        assertNull(emptyList<EntityTypeInfo>().merge())
    }

    @Test
    fun `merge should sum the attribute counts and merge the types of attribute type infos`() {
        val merged = listOf(
            attributeTypeInfo(1, BEEHIVE_TERM, AttributeType.Property),
            attributeTypeInfo(4, APIARY_TERM, AttributeType.LanguageProperty)
        ).merge()

        assertEquals(5, merged?.attributeCount)
        assertEquals(setOf(APIARY_TERM, BEEHIVE_TERM), merged?.typeNames)
        assertEquals(setOf(AttributeType.Property, AttributeType.LanguageProperty), merged?.attributeTypes)
    }

    private fun entityTypeInfo(entityCount: Int, attributeTypes: List<AttributeType>) =
        EntityTypeInfo(
            id = BEEHIVE_IRI.toUri(),
            typeName = BEEHIVE_TERM,
            entityCount = entityCount,
            attributeDetails = listOf(
                AttributeInfo(
                    TEMPERATURE_IRI.toUri(),
                    attributeName = TEMPERATURE_TERM,
                    attributeTypes = attributeTypes
                )
            )
        )

    private fun attributeTypeInfo(attributeCount: Int, typeName: String, attributeType: AttributeType) =
        AttributeTypeInfo(
            id = NAME_IRI.toUri(),
            attributeName = NAME_TERM,
            attributeTypes = setOf(attributeType),
            typeNames = setOf(typeName),
            attributeCount = attributeCount
        )
}
