package com.egm.stellio.search.csr.util

import com.egm.stellio.search.csr.CsrUtils.gimmeRawCSR
import com.egm.stellio.search.csr.model.EntityInfo
import com.egm.stellio.search.csr.model.RegistrationInfo
import com.egm.stellio.search.discovery.model.AttributeDetails
import com.egm.stellio.search.discovery.model.AttributeInfo
import com.egm.stellio.search.discovery.model.AttributeList
import com.egm.stellio.search.discovery.model.AttributeType
import com.egm.stellio.search.discovery.model.AttributeTypeInfo
import com.egm.stellio.search.discovery.model.EntityType
import com.egm.stellio.search.discovery.model.EntityTypeInfo
import com.egm.stellio.search.discovery.model.EntityTypeList
import com.egm.stellio.shared.util.APIARY_IRI
import com.egm.stellio.shared.util.APIARY_TERM
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.BEEHIVE_TERM
import com.egm.stellio.shared.util.NAME_IRI
import com.egm.stellio.shared.util.NAME_TERM
import com.egm.stellio.shared.util.SENSOR_IRI
import com.egm.stellio.shared.util.SENSOR_TERM
import com.egm.stellio.shared.util.TEMPERATURE_IRI
import com.egm.stellio.shared.util.TEMPERATURE_TERM
import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class DiscoveryRestrictionUtilsTests {

    // Apiary entities with the name attribute, and BeeHive entities with the temperature attribute
    private val csr = gimmeRawCSR(
        information = listOf(
            RegistrationInfo(
                entities = listOf(EntityInfo(types = listOf(APIARY_IRI))),
                propertyNames = listOf(NAME_IRI)
            ),
            RegistrationInfo(
                entities = listOf(EntityInfo(types = listOf(BEEHIVE_IRI))),
                propertyNames = listOf(TEMPERATURE_IRI)
            )
        )
    )

    @Test
    fun `restrictTo should only keep the registered types of an EntityTypeList`() {
        val entityTypeList = EntityTypeList(typeList = listOf(APIARY_TERM, BEEHIVE_TERM, SENSOR_TERM))

        val restricted = entityTypeList.restrictTo(csr, APIC_COMPOUND_CONTEXTS)

        assertEquals(listOf(APIARY_TERM, BEEHIVE_TERM), restricted.typeList)
    }

    @Test
    fun `restrictTo should only keep the attributes registered with each entity type`() {
        val entityTypes = listOf(
            entityType(APIARY_IRI, APIARY_TERM, NAME_TERM, TEMPERATURE_TERM),
            entityType(SENSOR_IRI, SENSOR_TERM, NAME_TERM)
        )

        val restricted = entityTypes.restrictTo(csr, APIC_COMPOUND_CONTEXTS)

        assertEquals(listOf(entityType(APIARY_IRI, APIARY_TERM, NAME_TERM)), restricted)
    }

    @Test
    fun `restrictTo should drop an entity type when none of its attributes is registered with it`() {
        val entityTypes = listOf(entityType(BEEHIVE_IRI, BEEHIVE_TERM, NAME_TERM))

        assertEquals(emptyList<EntityType>(), entityTypes.restrictTo(csr, APIC_COMPOUND_CONTEXTS))
    }

    @Test
    fun `restrictTo should keep a registered entity type without attributes`() {
        val entityTypes = listOf(entityType(BEEHIVE_IRI, BEEHIVE_TERM))

        assertEquals(entityTypes, entityTypes.restrictTo(csr, APIC_COMPOUND_CONTEXTS))
    }

    @Test
    fun `restrictTo should only keep the attributes registered with the type of an EntityTypeInfo`() {
        val entityTypeInfo = EntityTypeInfo(
            id = BEEHIVE_IRI.toUri(),
            typeName = BEEHIVE_TERM,
            entityCount = 2,
            attributeDetails = listOf(
                attributeInfo(NAME_IRI, NAME_TERM),
                attributeInfo(TEMPERATURE_IRI, TEMPERATURE_TERM)
            )
        )

        val restricted = entityTypeInfo.restrictTo(BEEHIVE_IRI, csr, APIC_COMPOUND_CONTEXTS)

        assertEquals(listOf(TEMPERATURE_TERM), restricted?.attributeDetails?.map { it.attributeName })
        assertEquals(2, restricted?.entityCount)
    }

    @Test
    fun `restrictTo should reject an EntityTypeInfo about another type than the requested one`() {
        val entityTypeInfo = EntityTypeInfo(
            id = APIARY_IRI.toUri(),
            typeName = APIARY_TERM,
            entityCount = 1,
            attributeDetails = emptyList()
        )

        assertNull(entityTypeInfo.restrictTo(BEEHIVE_IRI, csr, APIC_COMPOUND_CONTEXTS))
    }

    @Test
    fun `restrictTo should only keep the registered attributes of an AttributeList`() {
        val attributeList = AttributeList(attributeList = listOf(NAME_TERM, TEMPERATURE_TERM, "humidity"))

        val restricted = attributeList.restrictTo(csr, APIC_COMPOUND_CONTEXTS)

        assertEquals(listOf(NAME_TERM, TEMPERATURE_TERM), restricted.attributeList)
    }

    @Test
    fun `restrictTo should only keep the types registered with each attribute`() {
        val attributes = listOf(
            attributeDetails(NAME_IRI, NAME_TERM, APIARY_TERM, BEEHIVE_TERM),
            attributeDetails(TEMPERATURE_IRI, TEMPERATURE_TERM, APIARY_TERM)
        )

        val restricted = attributes.restrictTo(csr, APIC_COMPOUND_CONTEXTS)

        // temperature is only registered with BeeHive: it is dropped as its only type is Apiary
        assertEquals(listOf(attributeDetails(NAME_IRI, NAME_TERM, APIARY_TERM)), restricted)
    }

    @Test
    fun `restrictTo should only keep the types registered with the attribute of an AttributeTypeInfo`() {
        val attributeTypeInfo = attributeTypeInfo(TEMPERATURE_IRI, TEMPERATURE_TERM, APIARY_TERM, BEEHIVE_TERM)

        val restricted = attributeTypeInfo.restrictTo(TEMPERATURE_IRI, csr, APIC_COMPOUND_CONTEXTS)

        assertEquals(setOf(BEEHIVE_TERM), restricted?.typeNames)
    }

    @Test
    fun `restrictTo should reject an AttributeTypeInfo about another attribute than the requested one`() {
        val attributeTypeInfo = attributeTypeInfo(NAME_IRI, NAME_TERM, APIARY_TERM)

        assertNull(attributeTypeInfo.restrictTo(TEMPERATURE_IRI, csr, APIC_COMPOUND_CONTEXTS))
    }

    private fun entityType(id: String, typeName: String, vararg attributeNames: String) =
        EntityType(id.toUri(), typeName = typeName, attributeNames = attributeNames.toList())

    private fun attributeDetails(id: String, attributeName: String, vararg typeNames: String) =
        AttributeDetails(id.toUri(), attributeName = attributeName, typeNames = typeNames.toSet())

    private fun attributeInfo(id: String, attributeName: String) =
        AttributeInfo(id.toUri(), attributeName = attributeName, attributeTypes = listOf(AttributeType.Property))

    private fun attributeTypeInfo(id: String, attributeName: String, vararg typeNames: String) =
        AttributeTypeInfo(
            id = id.toUri(),
            attributeName = attributeName,
            attributeTypes = setOf(AttributeType.Property),
            typeNames = typeNames.toSet(),
            attributeCount = 3
        )
}
