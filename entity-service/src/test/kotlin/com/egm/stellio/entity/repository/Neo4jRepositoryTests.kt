package com.egm.stellio.entity.repository

import com.egm.stellio.entity.config.TestContainersConfiguration
import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.PartialEntity
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.entity.model.toRelationshipTypeName
import com.egm.stellio.shared.model.GeoPropertyType
import com.egm.stellio.shared.model.NgsiLdGeoPropertyInstance
import com.egm.stellio.shared.model.NgsiLdProperty
import com.egm.stellio.shared.model.parseToNgsiLdAttributes
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.EGM_OBSERVED_BY
import com.egm.stellio.shared.util.toUri
import junit.framework.TestCase.assertEquals
import junit.framework.TestCase.assertFalse
import junit.framework.TestCase.assertNotNull
import junit.framework.TestCase.assertNull
import junit.framework.TestCase.assertTrue
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.ActiveProfiles
import java.net.URI
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZonedDateTime

@SpringBootTest
@ActiveProfiles("test")
@Import(TestContainersConfiguration::class)
class Neo4jRepositoryTests {

    @Autowired
    private lateinit var neo4jRepository: Neo4jRepository

    @Autowired
    private lateinit var entityRepository: EntityRepository

    @Autowired
    private lateinit var propertyRepository: PropertyRepository

    @Autowired
    private lateinit var relationshipRepository: RelationshipRepository

    @Autowired
    private lateinit var partialEntityRepository: PartialEntityRepository

    private val beekeeperUri = "urn:ngsi-ld:Beekeeper:1230".toUri()
    private val deadFishUri = "urn:ngsi-ld:DeadFishes:019BN".toUri()
    private val partialTargetEntityUri = "urn:ngsi-ld:Entity:4567".toUri()

    @Test
    fun `it should merge a partial entity to a normal entity`() {
        val partialEntity = PartialEntity(beekeeperUri)
        partialEntityRepository.save(partialEntity)
        createEntity(beekeeperUri, listOf("Beekeeper"))

        neo4jRepository.mergePartialWithNormalEntity(beekeeperUri)

        assertFalse(partialEntityRepository.existsById(beekeeperUri))
        assertTrue(entityRepository.existsById(beekeeperUri))
        val beekeeper = entityRepository.findById(beekeeperUri)
        assertEquals(listOf("Beekeeper"), beekeeper.get().type)

        neo4jRepository.deleteEntity(beekeeperUri)
    }

    @Test
    fun `it should keep a relationship to a partial entity when merging with a normal entity`() {
        val partialEntity = PartialEntity(beekeeperUri)
        partialEntityRepository.save(partialEntity)
        val property = createProperty("https://ontology.eglobalmark.com/apic#outgoing", 1.0)
        createRelationship(AttributeSubjectNode(property.id), EGM_OBSERVED_BY, beekeeperUri)
        createEntity(beekeeperUri, listOf("Beekeeper"))

        neo4jRepository.mergePartialWithNormalEntity(beekeeperUri)

        val relsOfProp = propertyRepository.findById(property.id).get().relationships
        assertEquals(1, relsOfProp.size)
        assertEquals(EGM_OBSERVED_BY, relsOfProp[0].type[0])

        neo4jRepository.deleteEntity(beekeeperUri)
    }

    @Test
    fun `it should return an entity if type and string properties are correct`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val entities = neo4jRepository.getEntities(
            null,
            "Beekeeper",
            null,
            "name==\"Scalpa\""
        )
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an empty list if string properties are wrong`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val entities = neo4jRepository.getEntities(
            null, "Beekeeper", null, "name==\"ScalpaXYZ\""
        )
        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if type and integer properties are correct`() {
        val entity = createEntity(
            deadFishUri,
            listOf("DeadFishes"),
            mutableListOf(Property(name = "fishNumber", value = 500))
        )
        val entities = neo4jRepository.getEntities(
            null,
            "DeadFishes",
            null,
            "fishNumber==500"
        )
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an empty list if integer properties are wrong`() {
        val entity = createEntity(
            deadFishUri,
            listOf("DeadFishes"),
            mutableListOf(Property(name = "fishNumber", value = 500))
        )
        val entities = neo4jRepository.getEntities(
            null,
            "DeadFishes",
            null,
            "fishNumber==499"
        )
        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if type and float properties are correct`() {
        val entity = createEntity(
            deadFishUri,
            listOf("DeadFishes"),
            mutableListOf(Property(name = "fishWeight", value = 120.50))
        )
        val entities = neo4jRepository.getEntities(
            null,
            "DeadFishes",
            null,
            "fishWeight==120.50"
        )
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an empty list if float properties are wrong`() {
        val entity = createEntity(
            deadFishUri,
            listOf("DeadFishes"),
            mutableListOf(Property(name = "fishWeight", value = -120.50))
        )
        val entities = neo4jRepository.getEntities(
            null,
            "DeadFishes",
            null,
            "fishWeight==-120"
        )
        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an empty list if given weight equals to entity weight and comparison parameter is wrong`() {
        val entity = createEntity(
            deadFishUri,
            listOf("DeadFishes"),
            mutableListOf(Property(name = "fishWeight", value = 180.9))
        )
        val entities = neo4jRepository.getEntities(
            null,
            "DeadFishes",
            null,
            "fishWeight>180.9"
        )
        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if given weight equals to entity weight and comparison parameter is correct`() {
        val entity = createEntity(
            deadFishUri,
            listOf("DeadFishes"),
            mutableListOf(Property(name = "fishWeight", value = 255))
        )
        val entities = neo4jRepository.getEntities(
            null,
            "DeadFishes",
            null,
            "fishWeight>=255"
        )
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an empty list if given name equals to entity name and comparison parameter is wrong`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "ScalpaXYZ"))
        )
        val entities = neo4jRepository.getEntities(
            null,
            "Beekeeper",
            null,
            "name!=\"ScalpaXYZ\""
        )
        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if given name not equals to entity name and comparison parameter is correct`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val entities = neo4jRepository.getEntities(
            null,
            "Beekeeper",
            null,
            "name!=\"ScalpaXYZ\""
        )
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if type and dateTime properties are correct`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = "testedAt", value = ZonedDateTime.parse("2018-12-04T12:00:00Z")))
        )
        val entities = neo4jRepository.getEntities(
            null,
            "Beekeeper",
            null,
            "testedAt==2018-12-04T12:00:00Z"
        )
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if type and date properties are correct`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = "testedAt", value = LocalDate.parse("2018-12-04")))
        )
        val entities = neo4jRepository.getEntities(
            null,
            "Beekeeper",
            null,
            "testedAt==2018-12-04"
        )
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should not return an entity if type is correct but not the compared date`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = "testedAt", value = LocalDate.parse("2018-12-04")))
        )
        val entities = neo4jRepository.getEntities(
            null,
            "Beekeeper",
            null,
            "testedAt==2018-12-07"
        )
        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if type and time properties are correct`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = "testedAt", value = LocalTime.parse("12:00:00")))
        )
        val entities = neo4jRepository.getEntities(
            null,
            "Beekeeper",
            null,
            "testedAt==12:00:00"
        )
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if type, time and string are correct`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(
                Property(name = "testedAt", value = LocalTime.parse("12:00:00")),
                Property(name = "name", value = "beekeeper")
            )
        )
        val entities = neo4jRepository.getEntities(
            null,
            "Beekeeper",
            null,
            "testedAt==12:00:00;name==\"beekeeper\""
        )
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should not return an entity if one of time or string is not correct`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(
                Property(name = "testedAt", value = LocalTime.parse("12:00:00")),
                Property(name = "name", value = "beekeeper")
            )
        )

        var entities = neo4jRepository.getEntities(
            null,
            "Beekeeper",
            null,
            "testedAt==13:00:00;name==\"beekeeper\""
        )
        assertFalse(entities.contains(entity.id))

        entities = neo4jRepository.getEntities(
            null,
            "Beekeeper",
            null,
            "testedAt==12:00:00;name==\"beekeeperx\""
        )
        assertFalse(entities.contains(entity.id))

        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if type and combinations of time, string and relationship are correct`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(
                Property(name = "testedAt", value = LocalTime.parse("12:00:00")),
                Property(name = "name", value = "beekeeper")
            )
        )
        createRelationship(EntitySubjectNode(entity.id), "observedBy", partialTargetEntityUri)

        var entities = neo4jRepository.getEntities(
            null,
            "Beekeeper",
            null,
            "testedAt==12:00:00;observedBy==\"urn:ngsi-ld:Entity:4567\""
        )
        assertTrue(entities.contains(entity.id))

        entities = neo4jRepository.getEntities(
            null,
            "Beekeeper",
            null,
            "(testedAt==12:00:00;observedBy==\"urn:ngsi-ld:Entity:4567\");name==\"beekeeper\""
        )
        assertTrue(entities.contains(entity.id))

        entities = neo4jRepository.getEntities(
            null,
            "Beekeeper",
            null,
            "(testedAt==12:00:00;observedBy==\"urn:ngsi-ld:Entity:4567\")|name==\"beekeeper\""
        )
        assertTrue(entities.contains(entity.id))

        entities = neo4jRepository.getEntities(
            null,
            "Beekeeper",
            null,
            "(testedAt==13:00:00;observedBy==\"urn:ngsi-ld:Entity:4567\")|name==\"beekeeper\""
        )
        assertTrue(entities.contains(entity.id))

        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return entities matching given type and id`() {
        val firstEntity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:1231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa2"))
        )
        val entities = neo4jRepository.getEntities(
            listOf("urn:ngsi-ld:Beekeeper:1231"),
            "Beekeeper",
            null,
            ""
        )
        assertFalse(entities.contains(firstEntity.id))
        assertTrue(entities.contains(secondEntity.id))
        neo4jRepository.deleteEntity(firstEntity.id)
        neo4jRepository.deleteEntity(secondEntity.id)
    }

    @Test
    fun `it should return entities matching given type and ids`() {
        val firstEntity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:1231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa2"))
        )
        val thirdEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:1232".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa3"))
        )
        val entities = neo4jRepository.getEntities(
            listOf("urn:ngsi-ld:Beekeeper:1231", "urn:ngsi-ld:Beekeeper:1232"),
            "Beekeeper",
            null,
            ""
        )
        assertFalse(entities.contains(firstEntity.id))
        assertTrue(entities.contains(secondEntity.id))
        assertTrue(entities.contains(thirdEntity.id))
        neo4jRepository.deleteEntity(firstEntity.id)
        neo4jRepository.deleteEntity(secondEntity.id)
        neo4jRepository.deleteEntity(thirdEntity.id)
    }

    @Test
    fun `it should return entities matching given type and idPattern`() {
        val firstEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:01231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:01232".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa2"))
        )
        val thirdEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:11232".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa3"))
        )
        val entities = neo4jRepository.getEntities(
            null,
            "Beekeeper",
            "^urn:ngsi-ld:Beekeeper:0.*2$",
            ""
        )
        assertFalse(entities.contains(firstEntity.id))
        assertTrue(entities.contains(secondEntity.id))
        assertFalse(entities.contains(thirdEntity.id))
        neo4jRepository.deleteEntity(firstEntity.id)
        neo4jRepository.deleteEntity(secondEntity.id)
        neo4jRepository.deleteEntity(thirdEntity.id)
    }

    @Test
    fun `it should return no entity if given idPattern is not matching`() {
        val firstEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:01231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:01232".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa2"))
        )
        val thirdEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:11232".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa3"))
        )
        val entities = neo4jRepository.getEntities(
            null,
            "Beekeeper",
            "^urn:ngsi-ld:BeeHive:.*",
            ""
        )
        assertTrue(entities.isEmpty())
        neo4jRepository.deleteEntity(firstEntity.id)
        neo4jRepository.deleteEntity(secondEntity.id)
        neo4jRepository.deleteEntity(thirdEntity.id)
    }

    @Test
    fun `it should update the default property instance`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(
                Property(name = "https://uri.etsi.org/ngsi-ld/size", value = 100L),
                Property(
                    name = "https://uri.etsi.org/ngsi-ld/size",
                    value = 200L,
                    datasetId = "urn:ngsi-ld:Dataset:size:1".toUri()
                )
            )
        )

        val newPropertyPayload =
            """
            {
              "size": {
                "type": "Property",
                "value": 300
              }
            }
            """.trimIndent()
        val newProperty = parseToNgsiLdAttributes(
            JsonLdUtils.expandJsonLdFragment(newPropertyPayload, emptyList())
        )[0] as NgsiLdProperty

        neo4jRepository.updateEntityPropertyInstance(
            EntitySubjectNode(entity.id),
            "https://uri.etsi.org/ngsi-ld/size",
            newProperty.instances[0]
        )

        assertEquals(
            300L,
            neo4jRepository.getPropertyOfSubject(entity.id, "https://uri.etsi.org/ngsi-ld/size").value
        )
        assertEquals(
            200L,
            neo4jRepository.getPropertyOfSubject(
                entity.id, "https://uri.etsi.org/ngsi-ld/size", "urn:ngsi-ld:Dataset:size:1".toUri()
            ).value
        )

        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should update the property instance that matches the given datasetId`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(
                Property(name = "https://uri.etsi.org/ngsi-ld/size", value = 100L),
                Property(
                    name = "https://uri.etsi.org/ngsi-ld/size",
                    value = 200L,
                    datasetId = "urn:ngsi-ld:Dataset:size:1".toUri()
                )
            )
        )

        val newPropertyPayload =
            """
            {
              "size": {
                "type": "Property",
                "value": 300,
                "datasetId": "urn:ngsi-ld:Dataset:size:1"
              }
            }
            """.trimIndent()
        val newProperty = parseToNgsiLdAttributes(
            JsonLdUtils.expandJsonLdFragment(newPropertyPayload, emptyList())
        )[0] as NgsiLdProperty

        neo4jRepository.updateEntityPropertyInstance(
            EntitySubjectNode(entity.id),
            "https://uri.etsi.org/ngsi-ld/size",
            newProperty.instances[0]
        )

        assertEquals(
            100L,
            neo4jRepository.getPropertyOfSubject(entity.id, "https://uri.etsi.org/ngsi-ld/size").value
        )
        assertEquals(
            300L,
            neo4jRepository.getPropertyOfSubject(
                entity.id, "https://uri.etsi.org/ngsi-ld/size", "urn:ngsi-ld:Dataset:size:1".toUri()
            ).value
        )

        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should update properties of a property instance`() {
        val nameProperty = createProperty("https://uri.etsi.org/ngsi-ld/name", "Charity")
        val originProperty = createProperty("https://uri.etsi.org/ngsi-ld/origin", "Latin")
        nameProperty.properties.add(originProperty)
        propertyRepository.save(nameProperty)
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(nameProperty)
        )

        val newPropertyPayload =
            """
            {
              "name": {
                "type": "Property",
                "value": "Roman",
                "origin": {
                    "type": "Property",
                    "value": "English"
                }
              }
            }
            """.trimIndent()
        val newProperty = parseToNgsiLdAttributes(
            JsonLdUtils.expandJsonLdFragment(newPropertyPayload, emptyList())
        )[0] as NgsiLdProperty

        neo4jRepository.updateEntityPropertyInstance(
            EntitySubjectNode(entity.id),
            "https://uri.etsi.org/ngsi-ld/name",
            newProperty.instances[0]
        )

        val updatedPropertyId = neo4jRepository.getPropertyOfSubject(
            entity.id, "https://uri.etsi.org/ngsi-ld/name"
        ).id
        assertEquals(propertyRepository.findById(updatedPropertyId).get().properties[0].value, "English")

        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should create a relationship to an unknown entity as a partial entity`() {
        val entity = createEntity(
            "urn:ngsi-ld:Sensor:1233".toUri(),
            listOf("Sensor")
        )
        createRelationship(EntitySubjectNode(entity.id), EGM_OBSERVED_BY, partialTargetEntityUri)

        val somePartialEntity = partialEntityRepository.findById(partialTargetEntityUri)
        assertTrue(somePartialEntity.isPresent)
    }

    @Test
    fun `it should update relationships of a property instance`() {
        val temperatureProperty = createProperty("https://uri.etsi.org/ngsi-ld/temperature", 36)
        propertyRepository.save(temperatureProperty)

        val targetEntity = createEntity(
            "urn:ngsi-ld:Sensor:1233".toUri(),
            listOf("Sensor")
        )
        createRelationship(AttributeSubjectNode(temperatureProperty.id), EGM_OBSERVED_BY, targetEntity.id)
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(temperatureProperty)
        )

        createEntity("urn:ngsi-ld:Sensor:6789".toUri(), listOf("Sensor"))
        val newPropertyPayload =
            """
            {
              "temperature": {
                "type": "Property",
                "value": 58,
                "newRel": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:Sensor:6789"
                }
              }
            }
            """.trimIndent()
        val newProperty = parseToNgsiLdAttributes(
            JsonLdUtils.expandJsonLdFragment(newPropertyPayload, emptyList())
        )[0] as NgsiLdProperty

        neo4jRepository.updateEntityPropertyInstance(
            EntitySubjectNode(entity.id),
            "https://uri.etsi.org/ngsi-ld/temperature",
            newProperty.instances[0]
        )

        val updatedPropertyId = neo4jRepository.getPropertyOfSubject(
            entity.id, "https://uri.etsi.org/ngsi-ld/temperature"
        ).id
        assertEquals(
            propertyRepository.findById(updatedPropertyId).get().relationships[0].type[0],
            "https://uri.etsi.org/ngsi-ld/default-context/newRel"
        )

        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should add modifiedAt value when creating a new property`() {
        val property = Property(name = "name", value = "Scalpa")
        propertyRepository.save(property)
        assertNotNull(propertyRepository.findById(property.id).get().modifiedAt)
        propertyRepository.deleteById(property.id)
    }

    @Test
    fun `it should add modifiedAt value when saving a new relationship`() {
        val relationship = Relationship(type = listOf("connectsTo"))
        relationshipRepository.save(relationship)
        assertNotNull(relationshipRepository.findById(relationship.id).get().modifiedAt)
        relationshipRepository.deleteById(relationship.id)
    }

    @Test
    fun `it should update modifiedAt value when updating an entity`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val modifiedAt = entityRepository.findById("urn:ngsi-ld:Beekeeper:1233".toUri()).get().modifiedAt
        createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Demha"))
        )
        val updatedModifiedAt = entityRepository.findById("urn:ngsi-ld:Beekeeper:1233".toUri()).get().modifiedAt
        assertThat(updatedModifiedAt).isAfter(modifiedAt)
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should create createdAt property with time zone related information equal to the character "Z"`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        assertTrue(entity.createdAt.toString().endsWith("Z"))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should create modifiedAt property with time zone related information equal to the character "Z"`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        assertTrue(entity.modifiedAt.toString().endsWith("Z"))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should filter existing entityIds`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )

        val entitiesIds = listOf("urn:ngsi-ld:Beekeeper:1233".toUri(), "urn:ngsi-ld:Beekeeper:1234".toUri())

        assertEquals(
            listOf("urn:ngsi-ld:Beekeeper:1233".toUri()),
            neo4jRepository.filterExistingEntitiesAsIds(entitiesIds)
        )
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should delete all property instances`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(
                Property(name = "firstName", value = "Scalpa"),
                Property(
                    name = "firstName",
                    value = "Scalpa2",
                    datasetId = "urn:ngsi-ld:Dataset:firstName:2".toUri()
                ),
                Property(name = "lastName", value = "Charity")
            )
        )
        neo4jRepository.deleteEntityProperty(EntitySubjectNode(entity.id), "firstName", null, true)

        assertEquals(entityRepository.findById(entity.id).get().properties.size, 1)
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should delete the property default instance`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(
                Property(name = "firstName", value = "Scalpa"),
                Property(
                    name = "firstName",
                    value = "Scalpa2",
                    datasetId = "urn:ngsi-ld:Dataset:firstName:2".toUri()
                ),
                Property(name = "lastName", value = "Charity")
            )
        )

        neo4jRepository.deleteEntityProperty(EntitySubjectNode(entity.id), "firstName")

        val properties = entityRepository.findById(entity.id).get().properties
        assertNull(
            properties.find {
                it.name == "firstName" &&
                    it.value == "Scalpa" &&
                    it.datasetId == null
            }
        )
        assertNotNull(
            properties.find {
                it.name == "firstName" &&
                    it.value == "Scalpa2" &&
                    it.datasetId == "urn:ngsi-ld:Dataset:firstName:2".toUri()
            }
        )
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should delete the property instance with the given datasetId`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(
                Property(name = "firstName", value = "Scalpa"),
                Property(
                    name = "firstName",
                    value = "Scalpa2",
                    datasetId = "urn:ngsi-ld:Dataset:firstName:2".toUri()
                ),
                Property(name = "lastName", value = "Charity")
            )
        )

        neo4jRepository.deleteEntityProperty(
            EntitySubjectNode(entity.id),
            "firstName",
            "urn:ngsi-ld:Dataset:firstName:2".toUri()
        )

        val properties = entityRepository.findById(entity.id).get().properties
        assertNull(
            properties.find {
                it.name == "firstName" &&
                    it.value == "Scalpa2" &&
                    it.datasetId == "urn:ngsi-ld:Dataset:firstName:2".toUri()
            }
        )
        assertNotNull(
            properties.find {
                it.name == "firstName" &&
                    it.value == "Scalpa" &&
                    it.datasetId == null
            }
        )
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should delete all relationship instances`() {
        val sensor = createEntity("urn:ngsi-ld:Sensor:1233".toUri(), listOf("Sensor"))
        val device = createEntity("urn:ngsi-ld:Device:1233".toUri(), listOf("Device"))
        createRelationship(EntitySubjectNode(sensor.id), EGM_OBSERVED_BY, device.id)
        createRelationship(
            EntitySubjectNode(sensor.id), EGM_OBSERVED_BY, device.id, "urn:ngsi-ld:Dataset:observedBy:01".toUri()
        )

        neo4jRepository.deleteEntityRelationship(
            EntitySubjectNode(sensor.id),
            EGM_OBSERVED_BY.toRelationshipTypeName(),
            null,
            true
        )

        assertEquals(entityRepository.findById(sensor.id).get().relationships.size, 0)
        neo4jRepository.deleteEntity(sensor.id)
        neo4jRepository.deleteEntity(device.id)
    }

    @Test
    fun `it should delete the relationship default instance`() {
        val sensor = createEntity("urn:ngsi-ld:Sensor:1233".toUri(), listOf("Sensor"))
        val device = createEntity("urn:ngsi-ld:Device:1233".toUri(), listOf("Device"))
        createRelationship(EntitySubjectNode(sensor.id), EGM_OBSERVED_BY, device.id)
        createRelationship(
            EntitySubjectNode(sensor.id),
            EGM_OBSERVED_BY,
            device.id,
            "urn:ngsi-ld:Dataset:observedBy:01".toUri()
        )

        neo4jRepository.deleteEntityRelationship(EntitySubjectNode(sensor.id), EGM_OBSERVED_BY.toRelationshipTypeName())

        val relationships = entityRepository.findById(sensor.id).get().relationships
        assertNull(
            relationships.find {
                it.type == listOf(EGM_OBSERVED_BY) &&
                    it.datasetId == null
            }
        )
        assertNotNull(
            relationships.find {
                it.type == listOf(EGM_OBSERVED_BY) &&
                    it.datasetId == "urn:ngsi-ld:Dataset:observedBy:01".toUri()
            }
        )

        neo4jRepository.deleteEntity(sensor.id)
        neo4jRepository.deleteEntity(device.id)
    }

    @Test
    fun `it should delete the relationship instance with the given datasetId`() {
        val sensor = createEntity("urn:ngsi-ld:Sensor:1233".toUri(), listOf("Sensor"))
        val device = createEntity("urn:ngsi-ld:Device:1233".toUri(), listOf("Device"))
        createRelationship(EntitySubjectNode(sensor.id), EGM_OBSERVED_BY, device.id)
        createRelationship(
            EntitySubjectNode(sensor.id),
            EGM_OBSERVED_BY,
            device.id,
            "urn:ngsi-ld:Dataset:observedBy:01".toUri()
        )

        neo4jRepository.deleteEntityRelationship(
            EntitySubjectNode(sensor.id),
            EGM_OBSERVED_BY.toRelationshipTypeName(),
            "urn:ngsi-ld:Dataset:observedBy:01".toUri()
        )

        val relationships = entityRepository.findById(sensor.id).get().relationships
        assertNull(
            relationships.find {
                it.type == listOf(EGM_OBSERVED_BY) &&
                    it.datasetId == "urn:ngsi-ld:Dataset:observedBy:01".toUri()
            }
        )
        assertNotNull(
            relationships.find {
                it.type == listOf(EGM_OBSERVED_BY) &&
                    it.datasetId == null
            }
        )
        neo4jRepository.deleteEntity(sensor.id)
        neo4jRepository.deleteEntity(device.id)
    }

    @Test
    fun `it should delete an entity attribute and its properties`() {
        val lastNameProperty = createProperty("lastName", "Charity")
        val originProperty = createProperty("origin", "Latin")
        lastNameProperty.properties.add(originProperty)
        propertyRepository.save(lastNameProperty)
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "firstName", value = "Scalpa"), lastNameProperty)
        )

        neo4jRepository.deleteEntityProperty(EntitySubjectNode(entity.id), "lastName")

        assertTrue(propertyRepository.findById(originProperty.id).isEmpty)
        assertTrue(propertyRepository.findById(lastNameProperty.id).isEmpty)
        assertEquals(entityRepository.findById(entity.id).get().properties.size, 1)

        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should delete entity attributes`() {
        val sensor = createEntity(
            "urn:ngsi-ld:Sensor:1233".toUri(),
            listOf("Sensor"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val device = createEntity("urn:ngsi-ld:Device:1233".toUri(), listOf("Device"))
        createRelationship(EntitySubjectNode(sensor.id), EGM_OBSERVED_BY, device.id)

        neo4jRepository.deleteEntityAttributes(sensor.id)

        val entity = entityRepository.findById(sensor.id).get()
        assertEquals(entity.relationships.size, 0)
        assertEquals(entity.properties.size, 0)

        neo4jRepository.deleteEntity(sensor.id)
        neo4jRepository.deleteEntity(device.id)
    }

    @Test
    fun `it should delete incoming relationships when deleting an entity`() {
        val sensor = createEntity("urn:ngsi-ld:Sensor:1233".toUri(), listOf("Sensor"))
        val device = createEntity("urn:ngsi-ld:Device:1233".toUri(), listOf("Device"))
        createRelationship(EntitySubjectNode(device.id), EGM_OBSERVED_BY, sensor.id)

        neo4jRepository.deleteEntity(sensor.id)

        val entity = entityRepository.findById(device.id).get()
        assertEquals(entity.relationships.size, 0)

        neo4jRepository.deleteEntity(device.id)
    }

    @Test
    fun `it should return the default property instance if no datasetId is provided`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = 100L))
        )

        assertNotNull(neo4jRepository.getPropertyOfSubject(entity.id, "name", null))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should not return a default property instance if a datasetId is provided`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = 100L, datasetId = "urn:ngsi-ld:Dataset:name:1".toUri()))
        )

        assertThrows<NoSuchElementException>("List is empty") {
            neo4jRepository.getPropertyOfSubject(entity.id, "name", null)
        }
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return the property instance with the given datasetId`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = 100L, datasetId = "urn:ngsi-ld:Dataset:name:1".toUri()))
        )

        assertNotNull(
            neo4jRepository.getPropertyOfSubject(
                entity.id,
                "name",
                datasetId = "urn:ngsi-ld:Dataset:name:1".toUri()
            )
        )
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should not return the property instance if the given datasetId does not match`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = 100L, datasetId = "urn:ngsi-ld:Dataset:name:1".toUri()))
        )

        assertThrows<NoSuchElementException>("List is empty") {
            neo4jRepository.getPropertyOfSubject(
                entity.id,
                "name",
                datasetId = "urn:ngsi-ld:Dataset:name:2".toUri()
            )
        }
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return true if no datasetId is provided and a default instance exists`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = 100L))
        )

        assertTrue(neo4jRepository.hasPropertyInstance(EntitySubjectNode(entity.id), "name", null))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return false if no datasetId is provided and there is no default instance`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = 100L, datasetId = "urn:ngsi-ld:Dataset:name:1".toUri()))
        )

        assertFalse(neo4jRepository.hasPropertyInstance(EntitySubjectNode(entity.id), "name", null))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return true if there is an instance with the provided datasetId`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = 100L, datasetId = "urn:ngsi-ld:Dataset:name:1".toUri()))
        )

        assertTrue(
            neo4jRepository.hasPropertyInstance(
                EntitySubjectNode(entity.id),
                "name",
                "urn:ngsi-ld:Dataset:name:1".toUri()
            )
        )
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return false if there is no instance with the provided datasetId`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = 100L, datasetId = "urn:ngsi-ld:Dataset:name:1".toUri()))
        )

        assertFalse(
            neo4jRepository.hasPropertyInstance(
                EntitySubjectNode(entity.id),
                "name",
                "urn:ngsi-ld:Dataset:name:2".toUri()
            )
        )
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return true if no datasetId is provided and a default relationship instance exists`() {
        val sensor = createEntity("urn:ngsi-ld:Sensor:1233".toUri(), listOf("Sensor"))
        val device = createEntity("urn:ngsi-ld:Device:1233".toUri(), listOf("Device"))
        createRelationship(EntitySubjectNode(sensor.id), EGM_OBSERVED_BY, device.id)

        assertTrue(
            neo4jRepository.hasRelationshipInstance(
                EntitySubjectNode(sensor.id),
                EGM_OBSERVED_BY.toRelationshipTypeName()
            )
        )
        neo4jRepository.deleteEntity(sensor.id)
        neo4jRepository.deleteEntity(device.id)
    }

    @Test
    fun `it should return false if no datasetId is provided and there is no default relationship instance`() {
        val sensor = createEntity("urn:ngsi-ld:Sensor:1233".toUri(), listOf("Sensor"))
        val device = createEntity("urn:ngsi-ld:Device:1233".toUri(), listOf("Device"))
        createRelationship(
            EntitySubjectNode(sensor.id),
            EGM_OBSERVED_BY,
            device.id,
            "urn:ngsi-ld:Dataset:observedBy:01".toUri()
        )

        assertFalse(
            neo4jRepository.hasRelationshipInstance(
                EntitySubjectNode(sensor.id), EGM_OBSERVED_BY.toRelationshipTypeName()
            )
        )
        neo4jRepository.deleteEntity(sensor.id)
        neo4jRepository.deleteEntity(device.id)
    }

    @Test
    fun `it should return true if there is a relationship instance with the provided datasetId`() {
        val sensor = createEntity("urn:ngsi-ld:Sensor:1233".toUri(), listOf("Sensor"))
        val device = createEntity("urn:ngsi-ld:Device:1233".toUri(), listOf("Device"))
        createRelationship(
            EntitySubjectNode(sensor.id),
            EGM_OBSERVED_BY,
            device.id,
            "urn:ngsi-ld:Dataset:observedBy:01".toUri()
        )

        assertTrue(
            neo4jRepository.hasRelationshipInstance(
                EntitySubjectNode(sensor.id),
                EGM_OBSERVED_BY.toRelationshipTypeName(),
                "urn:ngsi-ld:Dataset:observedBy:01".toUri()
            )
        )
        neo4jRepository.deleteEntity(sensor.id)
        neo4jRepository.deleteEntity(device.id)
    }

    @Test
    fun `it should return false if there is no relationship instance with the provided datasetId`() {
        val sensor = createEntity("urn:ngsi-ld:Sensor:1233".toUri(), listOf("Sensor"))
        val device = createEntity("urn:ngsi-ld:Device:1233".toUri(), listOf("Device"))
        createRelationship(
            EntitySubjectNode(sensor.id),
            EGM_OBSERVED_BY,
            device.id,
            "urn:ngsi-ld:Dataset:observedBy:01".toUri()
        )

        assertFalse(
            neo4jRepository.hasRelationshipInstance(
                EntitySubjectNode(sensor.id),
                EGM_OBSERVED_BY.toRelationshipTypeName(),
                "urn:ngsi-ld:Dataset:observedBy:0002".toUri()
            )
        )
        neo4jRepository.deleteEntity(sensor.id)
        neo4jRepository.deleteEntity(device.id)
    }

    @Test
    fun `it should update the relationship target of an entity`() {
        val sensor = createEntity("urn:ngsi-ld:Sensor:1233".toUri(), listOf("Sensor"), mutableListOf())
        val device = createEntity("urn:ngsi-ld:Device:1233".toUri(), listOf("Device"), mutableListOf())
        val newDevice = createEntity("urn:ngsi-ld:Device:9978".toUri(), listOf("Device"), mutableListOf())
        createRelationship(
            EntitySubjectNode(sensor.id),
            EGM_OBSERVED_BY,
            device.id
        )

        neo4jRepository.updateRelationshipTargetOfSubject(
            sensor.id,
            EGM_OBSERVED_BY.toRelationshipTypeName(),
            newDevice.id
        )

        assertEquals(
            neo4jRepository.getRelationshipTargetOfSubject(sensor.id, EGM_OBSERVED_BY.toRelationshipTypeName())!!.id,
            newDevice.id
        )
        neo4jRepository.deleteEntity(sensor.id)
        neo4jRepository.deleteEntity(device.id)
        neo4jRepository.deleteEntity(newDevice.id)
    }

    @Test
    fun `it should return an emptyMap for unknown type`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beehive:TESTC".toUri(),
            listOf("https://ontology.eglobalmark.com/apic#Beehive"),
            mutableListOf(
                Property(name = "temperature", value = 36),
                Property(name = "humidity", value = 65)
            )
        )

        val attributesInformation = neo4jRepository
            .getEntityTypeAttributesInformation("https://ontology.eglobalmark.com/apic#unknownType")

        assertTrue(attributesInformation.isEmpty())

        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should retrieve entity type attributes information for two entities`() {
        val firstEntity = createEntity(
            "urn:ngsi-ld:Beehive:TESTC".toUri(),
            listOf("https://ontology.eglobalmark.com/apic#Beehive"),
            mutableListOf(
                Property(name = "temperature", value = 36),
                Property(name = "humidity", value = 65)
            )
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beehive:TESTB".toUri(),
            listOf("https://ontology.eglobalmark.com/apic#Beehive"),
            mutableListOf(
                Property(name = "temperature", value = 30),
                Property(name = "incoming", value = 61)
            )
        )

        val attributesInformation = neo4jRepository
            .getEntityTypeAttributesInformation("https://ontology.eglobalmark.com/apic#Beehive")

        val propertiesInformation = attributesInformation["properties"] as Set<*>

        assertEquals(propertiesInformation.size, 3)
        assertTrue(propertiesInformation.containsAll(listOf("humidity", "temperature", "incoming")))
        assertEquals(attributesInformation["relationships"], emptySet<String>())
        assertEquals(attributesInformation["geoProperties"], emptySet<String>())
        assertEquals(attributesInformation["entityCount"], 2)

        neo4jRepository.deleteEntity(firstEntity.id)
        neo4jRepository.deleteEntity(secondEntity.id)
    }

    @Test
    fun `it should retrieve entity type attributes information for two entities with relationships`() {
        val firstEntity = createEntity(
            "urn:ngsi-ld:Beehive:TESTC".toUri(),
            listOf("https://ontology.eglobalmark.com/apic#Beehive"),
            mutableListOf(
                Property(name = "temperature", value = 36),
                Property(name = "humidity", value = 65)
            )
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beehive:TESTB".toUri(),
            listOf("https://ontology.eglobalmark.com/apic#Beehive"),
            mutableListOf(
                Property(name = "temperature", value = 30),
                Property(name = "humidity", value = 61)
            )
        )
        createRelationship(EntitySubjectNode(secondEntity.id), "observedBy", partialTargetEntityUri)

        val attributesInformation = neo4jRepository
            .getEntityTypeAttributesInformation("https://ontology.eglobalmark.com/apic#Beehive")

        val propertiesInformation = attributesInformation["properties"] as Set<*>

        assertEquals(propertiesInformation.size, 2)
        assertTrue(propertiesInformation.containsAll(listOf("humidity", "temperature")))
        assertEquals(attributesInformation["relationships"], setOf("observedBy"))
        assertEquals(attributesInformation["geoProperties"], emptySet<String>())
        assertEquals(attributesInformation["entityCount"], 2)

        neo4jRepository.deleteEntity(firstEntity.id)
        neo4jRepository.deleteEntity(secondEntity.id)
    }

    @Test
    fun `it should retrieve entity type attributes information for three entities with location`() {
        val firstEntity = createEntity(
            "urn:ngsi-ld:Beehive:TESTC".toUri(),
            listOf("https://ontology.eglobalmark.com/apic#Beehive"),
            mutableListOf(
                Property(name = "temperature", value = 36),
                Property(name = "humidity", value = 65)
            ),
            NgsiLdGeoPropertyInstance.toWktFormat(GeoPropertyType.Point, listOf(24.30623, 60.07966))
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beehive:TESTB".toUri(),
            listOf("https://ontology.eglobalmark.com/apic#Beehive"),
            mutableListOf(
                Property(name = "temperature", value = 30),
                Property(name = "humidity", value = 61)
            )
        )
        val thirdEntity = createEntity(
            "urn:ngsi-ld:Beehive:TESTD".toUri(),
            listOf("https://ontology.eglobalmark.com/apic#Beehive"),
            mutableListOf(
                Property(name = "temperature", value = 25),
                Property(name = "humidity", value = 89)
            )
        )

        val attributesInformation = neo4jRepository
            .getEntityTypeAttributesInformation("https://ontology.eglobalmark.com/apic#Beehive")

        val propertiesInformation = attributesInformation["properties"] as Set<*>

        assertEquals(propertiesInformation.size, 2)
        assertTrue(propertiesInformation.containsAll(listOf("humidity", "temperature")))
        assertEquals(attributesInformation["relationships"], emptySet<String>())
        assertEquals(attributesInformation["geoProperties"], setOf("https://uri.etsi.org/ngsi-ld/location"))
        assertEquals(attributesInformation["entityCount"], 3)

        neo4jRepository.deleteEntity(firstEntity.id)
        neo4jRepository.deleteEntity(secondEntity.id)
        neo4jRepository.deleteEntity(thirdEntity.id)
    }

    @Test
    fun `it should retrieve entity types names`() {
        val firstEntity = createEntity(
            "urn:ngsi-ld:Beehive:TESTC".toUri(),
            listOf("https://ontology.eglobalmark.com/apic#Beehive"),
            mutableListOf(
                Property(name = "temperature", value = 36),
                Property(name = "humidity", value = 65)
            )
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Sensor:TESTC".toUri(),
            listOf("https://ontology.eglobalmark.com/apic#Sensor"),
            mutableListOf(
                Property(name = "deviceParameter", value = 30),
                Property(name = "isContainedIn", value = 61)
            )
        )
        val thirdEntity = createEntity(
            "urn:ngsi-ld:Sensor:TESTB".toUri(),
            listOf("https://ontology.eglobalmark.com/apic#Sensor"),
            mutableListOf(
                Property(name = "deviceParameter", value = 30),
                Property(name = "isContainedIn", value = 61)
            )
        )

        createRelationship(EntitySubjectNode(firstEntity.id), "observedBy", secondEntity.id)

        val entityTypesNames = neo4jRepository.getEntityTypesNames()

        assertEquals(entityTypesNames.size, 2)
        assertTrue(
            entityTypesNames.containsAll(
                listOf("https://ontology.eglobalmark.com/apic#Beehive", "https://ontology.eglobalmark.com/apic#Sensor")
            )
        )

        neo4jRepository.deleteEntity(firstEntity.id)
        neo4jRepository.deleteEntity(secondEntity.id)
        neo4jRepository.deleteEntity(thirdEntity.id)
    }

    @Test
    fun `it should retrieve a list of entity types`() {
        val firstEntity = createEntity(
            "urn:ngsi-ld:Beehive:TESTC".toUri(),
            listOf("https://ontology.eglobalmark.com/apic#Beehive"),
            mutableListOf(
                Property(name = "temperature", value = 36),
                Property(name = "humidity", value = 65)
            )
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beehive:TESTB".toUri(),
            listOf("https://ontology.eglobalmark.com/apic#Beehive"),
            mutableListOf(
                Property(name = "temperature", value = 36),
                Property(name = "name", value = "Beehive TESTB")
            ),
            NgsiLdGeoPropertyInstance.toWktFormat(GeoPropertyType.Point, listOf(24.30623, 60.07966))
        )
        val thirdEntity = createEntity(
            "urn:ngsi-ld:Sensor:TESTC".toUri(),
            listOf("https://ontology.eglobalmark.com/apic#Sensor"),
            mutableListOf(
                Property(name = "deviceParameter", value = 30)
            )
        )
        createRelationship(EntitySubjectNode(firstEntity.id), "observedBy", thirdEntity.id)

        val entityTypes = neo4jRepository.getEntityTypes()

        assertEquals(entityTypes.size, 2)
        assertTrue(
            entityTypes.containsAll(
                listOf(
                    mapOf(
                        "entityType" to "https://ontology.eglobalmark.com/apic#Beehive",
                        "properties" to setOf("temperature", "humidity", "name"),
                        "relationships" to setOf("observedBy"),
                        "geoProperties" to setOf("https://uri.etsi.org/ngsi-ld/location")
                    ),
                    mapOf(
                        "entityType" to "https://ontology.eglobalmark.com/apic#Sensor",
                        "properties" to setOf("deviceParameter"),
                        "relationships" to emptySet<String>(),
                        "geoProperties" to emptySet<String>()
                    )
                )
            )
        )

        neo4jRepository.deleteEntity(firstEntity.id)
        neo4jRepository.deleteEntity(secondEntity.id)
        neo4jRepository.deleteEntity(thirdEntity.id)
    }

    fun createEntity(
        id: URI,
        type: List<String>,
        properties: MutableList<Property> = mutableListOf(),
        location: String? = null
    ): Entity {
        val entity = Entity(id = id, type = type, properties = properties, location = location)
        return entityRepository.save(entity)
    }

    fun createProperty(name: String, value: Any): Property {
        val property = Property(name = name, value = value)
        return propertyRepository.save(property)
    }

    fun createRelationship(
        subjectNodeInfo: SubjectNodeInfo,
        relationshipType: String,
        objectId: URI,
        datasetId: URI? = null
    ): Relationship {
        val relationship = Relationship(type = listOf(relationshipType), datasetId = datasetId)
        neo4jRepository.createRelationshipOfSubject(subjectNodeInfo, relationship, objectId)

        return relationship
    }
}
