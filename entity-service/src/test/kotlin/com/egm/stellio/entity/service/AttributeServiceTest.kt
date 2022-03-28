package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.AttributeDetails
import com.egm.stellio.entity.repository.Neo4jRepository
import com.egm.stellio.shared.util.*
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [AttributeService::class])
@ActiveProfiles("test")
class AttributeServiceTest {

    @Autowired
    private lateinit var attributeService: AttributeService

    @MockkBean(relaxed = true)
    private lateinit var neo4jRepository: Neo4jRepository

    @Test
    fun `it should return a list of AttributeDetails`() {
        every { neo4jRepository.getAttributesDetails() } returns listOf(
            mapOf(
                "attribute" to TEMPERATURE_PROPERTY,
                "typeNames" to setOf(BEEHIVE_TYPE)
            ),
            mapOf(
                "attribute" to INCOMING_PROPERTY,
                "typeNames" to setOf(APIARY_TYPE)
            )
        )

        val attributeDetails = attributeService.getAttributeDetails(listOf(APIC_COMPOUND_CONTEXT))
        assertEquals(2, attributeDetails.size)
        assert(
            attributeDetails.containsAll(
                listOf(
                    AttributeDetails(
                        TEMPERATURE_PROPERTY.toUri(),
                        "Attribute",
                        TEMPERATURE_COMPACT_PROPERTY,
                        setOf(BEEHIVE_COMPACT_TYPE)
                    ),
                    AttributeDetails(
                        INCOMING_PROPERTY.toUri(),
                        "Attribute",
                        INCOMING_COMPACT_PROPERTY,
                        setOf(APIARY_COMPACT_TYPE)
                    )
                )
            )
        )
    }

    @Test
    fun `it should return an empty list of AttributeDetails if no attribute was found`() {
        every { neo4jRepository.getAttributesDetails() } returns emptyList()

        val attributeDetail = attributeService.getAttributeDetails(listOf(APIC_COMPOUND_CONTEXT))

        assertTrue(attributeDetail.isEmpty())
    }

    @Test
    fun `it should return an AttributeList`() {
        every { neo4jRepository.getAttributes() } returns listOf(TEMPERATURE_PROPERTY, INCOMING_PROPERTY)

        val attributeName = attributeService.getAttributeList(listOf(APIC_COMPOUND_CONTEXT))

        assert(attributeName.attributeList == listOf(TEMPERATURE_COMPACT_PROPERTY, INCOMING_COMPACT_PROPERTY))
    }

    @Test
    fun `it should return an attribute Information`() {
        every { neo4jRepository.getAttributeInformation(any()) } returns mapOf(
            "attributeName" to TEMPERATURE_PROPERTY,
            "attributeTypes" to setOf("Property"),
            "typeNames" to setOf(BEEHIVE_TYPE),
            "attributeCount" to 2
        )

        val attributeTypeInfo = attributeService.getAttributeTypeInfo(TEMPERATURE_PROPERTY, APIC_COMPOUND_CONTEXT)

        assertNotNull(attributeTypeInfo)
        assertEquals(TEMPERATURE_PROPERTY.toUri(), attributeTypeInfo!!.id)
        assertEquals("Attribute", attributeTypeInfo.type)
        assertEquals(TEMPERATURE_COMPACT_PROPERTY, attributeTypeInfo.attributeName)
        assertEquals(setOf("Property"), attributeTypeInfo.attributeTypes)
        assertEquals(setOf(BEEHIVE_COMPACT_TYPE), attributeTypeInfo.typeNames)
        assertEquals(2, attributeTypeInfo.attributeCount)
    }
}
