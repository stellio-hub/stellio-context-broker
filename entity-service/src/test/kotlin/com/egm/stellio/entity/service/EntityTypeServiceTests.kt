package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.AttributeInfo
import com.egm.stellio.entity.model.AttributeType
import com.egm.stellio.entity.repository.Neo4jRepository
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [EntityTypeService::class])
@ActiveProfiles("test")
class EntityTypeServiceTests {

    @Autowired
    private lateinit var entityTypeService: EntityTypeService

    @MockkBean(relaxed = true)
    private lateinit var neo4jRepository: Neo4jRepository

    @Test
    fun `it should return an EntityTypeInfo for one beehive entity with no attributeDetails`() {
        every { neo4jRepository.getEntityTypeAttributesInformation(any()) } returns mapOf(
            "properties" to emptySet<String>(),
            "relationships" to emptySet<String>(),
            "geoProperties" to emptySet<String>(),
            "entityCount" to 1
        )

        val entityTypeInfo = entityTypeService.getEntityTypeInfo(
            "https://ontology.eglobalmark.com/apic#BeeHive",
            listOf(APIC_COMPOUND_CONTEXT)
        )!!

        assert(entityTypeInfo.id == "https://ontology.eglobalmark.com/apic#BeeHive".toUri())
        assert(entityTypeInfo.type == "EntityTypeInfo")
        assert(entityTypeInfo.typeName == "BeeHive")
        assert(entityTypeInfo.entityCount == 1)
        assert(entityTypeInfo.attributeDetails.isEmpty())
    }

    @Test
    fun `it should return an EntityTypeInfo with an attributeDetails with one attributeInfo`() {
        every { neo4jRepository.getEntityTypeAttributesInformation(any()) } returns mapOf(
            "properties" to setOf("https://ontology.eglobalmark.com/apic#temperature"),
            "relationships" to emptySet<String>(),
            "geoProperties" to emptySet<String>(),
            "entityCount" to 2
        )

        val entityTypeInfo = entityTypeService.getEntityTypeInfo(
            "https://ontology.eglobalmark.com/apic#BeeHive",
            listOf(APIC_COMPOUND_CONTEXT)
        )!!

        assert(entityTypeInfo.id == "https://ontology.eglobalmark.com/apic#BeeHive".toUri())
        assert(entityTypeInfo.type == "EntityTypeInfo")
        assert(entityTypeInfo.typeName == "BeeHive")
        assert(entityTypeInfo.entityCount == 2)
        assert(entityTypeInfo.attributeDetails.size == 1)
        assert(
            entityTypeInfo.attributeDetails.contains(
                AttributeInfo(
                    "https://ontology.eglobalmark.com/apic#temperature".toUri(),
                    "Attribute",
                    "temperature",
                    listOf(AttributeType.Property)
                )
            )
        )
    }

    @Test
    fun `it should return an EntityTypeInfo with an attributeDetails with more than one attributeInfo`() {
        every { neo4jRepository.getEntityTypeAttributesInformation(any()) } returns mapOf(
            "properties" to setOf("https://ontology.eglobalmark.com/apic#temperature"),
            "relationships" to setOf("https://ontology.eglobalmark.com/egm#managedBy"),
            "geoProperties" to setOf("https://uri.etsi.org/ngsi-ld/location"),
            "entityCount" to 3
        )

        val entityTypeInfo = entityTypeService.getEntityTypeInfo(
            "https://ontology.eglobalmark.com/apic#BeeHive",
            listOf(APIC_COMPOUND_CONTEXT)
        )!!

        assert(entityTypeInfo.id == "https://ontology.eglobalmark.com/apic#BeeHive".toUri())
        assert(entityTypeInfo.type == "EntityTypeInfo")
        assert(entityTypeInfo.typeName == "BeeHive")
        assert(entityTypeInfo.entityCount == 3)
        assert(entityTypeInfo.attributeDetails.size == 3)
        assert(
            entityTypeInfo.attributeDetails.containsAll(
                listOf(
                    AttributeInfo(
                        "https://ontology.eglobalmark.com/apic#temperature".toUri(),
                        "Attribute",
                        "temperature",
                        listOf(AttributeType.Property)
                    ),
                    AttributeInfo(
                        "https://ontology.eglobalmark.com/egm#managedBy".toUri(),
                        "Attribute",
                        "managedBy",
                        listOf(AttributeType.Relationship)
                    ),
                    AttributeInfo(
                        "https://uri.etsi.org/ngsi-ld/location".toUri(),
                        "Attribute",
                        "location",
                        listOf(AttributeType.GeoProperty)
                    )
                )
            )
        )
    }
}
