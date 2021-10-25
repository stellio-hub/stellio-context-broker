package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.AttributeDetails
import com.egm.stellio.entity.repository.Neo4jRepository
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
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

    private val temperature = "https://ontology.eglobalmark.com/apic#temperature"
    private val deviceParameter = "https://ontology.eglobalmark.com/apic#deviceParameter"
    private val beeHiveType = "https://ontology.eglobalmark.com/apic#BeeHive"

    @Test
    fun `it should return a list of AttributeDetails`() {
        every { neo4jRepository.getAttributeDetails() } returns listOf(
            mapOf(
                "attribute" to temperature,
                "typeNames" to setOf("https://ontology.eglobalmark.com/apic#Beehive")
            ),
            mapOf(
                "attribute" to deviceParameter,
                "typeNames" to setOf("https://ontology.eglobalmark.com/apic#deviceParameter")
            )
        )

        val attributeDetails = attributeService.getAttributeDetails(listOf(APIC_COMPOUND_CONTEXT))
        assert(attributeDetails.size == 2)
        assert(
            attributeDetails.containsAll(
                listOf(
                    AttributeDetails(
                        temperature.toUri(),
                        "Attribute",
                        "temperature",
                        listOf(
                            "https://ontology.eglobalmark.com/apic#Beehive"
                        )
                    ),
                    AttributeDetails(
                        deviceParameter.toUri(),
                        "Attribute",
                        "https://ontology.eglobalmark.com/apic#deviceParameter",
                        listOf(
                            "https://ontology.eglobalmark.com/apic#deviceParameter"
                        )
                    )
                )
            )
        )
    }

    @Test
    fun `it should return an empty list of AttributeDetails if no attribute was found`() {
        every { neo4jRepository.getAttributeDetails() } returns emptyList()

        val attributeDetail = attributeService.getAttributeDetails(listOf(APIC_COMPOUND_CONTEXT))

        assert(attributeDetail.isEmpty())
    }

    @Test
    fun `it should return an AttributeList`() {
        every { neo4jRepository.getAttribute() } returns listOf(temperature, beeHiveType)

        val attributeName = attributeService.getAttributeList(listOf(APIC_COMPOUND_CONTEXT))

        assert(attributeName.attributeList == listOf("temperature", "BeeHive"))
    }
}
