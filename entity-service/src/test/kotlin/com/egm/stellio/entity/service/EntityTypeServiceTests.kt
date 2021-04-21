package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.AttributeInfo
import com.egm.stellio.entity.model.AttributeType
import com.egm.stellio.entity.model.EntityType
import com.egm.stellio.entity.repository.Neo4jRepository
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.AQUAC_COMPOUND_CONTEXT
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

    private val deadFishesType = "https://ontology.eglobalmark.com/aquac#DeadFishes"
    private val sensorType = "https://ontology.eglobalmark.com/egm#Sensor"
    private val beeHiveType = "https://ontology.eglobalmark.com/apic#BeeHive"

    @Test
    fun `it should return a list of EntityType`() {
        every { neo4jRepository.getEntityTypes() } returns listOf(
            mapOf(
                "entityType" to deadFishesType,
                "properties" to setOf("https://ontology.eglobalmark.com/aquac#fishNumber"),
                "relationships" to setOf("https://ontology.eglobalmark.com/aquac#removedFrom"),
                "geoProperties" to emptySet<String>()
            ),
            mapOf(
                "entityType" to sensorType,
                "properties" to setOf("https://ontology.eglobalmark.com/aquac#deviceParameter"),
                "relationships" to setOf("https://ontology.eglobalmark.com/aquac#isContainedIn"),
                "geoProperties" to emptySet<String>()
            )
        )

        val entityTypes = entityTypeService.getEntityTypes(listOf(AQUAC_COMPOUND_CONTEXT))

        assert(entityTypes.size == 2)
        assert(
            entityTypes.containsAll(
                listOf(
                    EntityType(
                        deadFishesType.toUri(),
                        "EntityType",
                        "DeadFishes",
                        listOf(
                            "https://ontology.eglobalmark.com/aquac#fishNumber",
                            "https://ontology.eglobalmark.com/aquac#removedFrom"
                        )
                    ),
                    EntityType(
                        sensorType.toUri(),
                        "EntityType",
                        "Sensor",
                        listOf(
                            "https://ontology.eglobalmark.com/aquac#deviceParameter",
                            "https://ontology.eglobalmark.com/aquac#isContainedIn"
                        )
                    )
                )
            )
        )
    }

    @Test
    fun `it should return an empty list of EntityTypes if no entity was found`() {
        every { neo4jRepository.getEntityTypes() } returns emptyList()

        val entityTypes = entityTypeService.getEntityTypes(listOf(AQUAC_COMPOUND_CONTEXT))

        assert(entityTypes.isEmpty())
    }

    @Test
    fun `it should return an EntityTypeList`() {
        every { neo4jRepository.getEntityTypesNames() } returns listOf(deadFishesType, sensorType)

        val entityTypes = entityTypeService.getEntityTypeList(listOf(AQUAC_COMPOUND_CONTEXT))

        assert(entityTypes.typeList == listOf(deadFishesType, sensorType))
    }

    @Test
    fun `it should return an EntityTypeInfo for one beehive entity with no attributeDetails`() {
        every { neo4jRepository.getEntityTypeAttributesInformation(any()) } returns mapOf(
            "properties" to emptySet<String>(),
            "relationships" to emptySet<String>(),
            "geoProperties" to emptySet<String>(),
            "entityCount" to 1
        )

        val entityTypeInfo = entityTypeService.getEntityTypeInfo(beeHiveType, listOf(APIC_COMPOUND_CONTEXT))!!

        assert(entityTypeInfo.id == beeHiveType.toUri())
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

        val entityTypeInfo = entityTypeService.getEntityTypeInfo(beeHiveType, listOf(APIC_COMPOUND_CONTEXT))!!

        assert(entityTypeInfo.id == beeHiveType.toUri())
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

        val entityTypeInfo = entityTypeService.getEntityTypeInfo(beeHiveType, listOf(APIC_COMPOUND_CONTEXT))!!

        assert(entityTypeInfo.id == beeHiveType.toUri())
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
