package com.egm.stellio.entity.repository

import com.egm.stellio.entity.config.WithNeo4jContainer
import com.egm.stellio.entity.model.*
import com.egm.stellio.shared.model.NgsiLdGeoPropertyInstance
import com.egm.stellio.shared.model.WKTCoordinates
import com.egm.stellio.shared.util.JsonLdUtils.EGM_OBSERVED_BY
import com.egm.stellio.shared.util.toUri
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.dao.EmptyResultDataAccessException
import org.springframework.test.context.ActiveProfiles
import java.net.URI

@SpringBootTest
@ActiveProfiles("test")
class Neo4jRepositoryTests : WithNeo4jContainer {

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
    private val partialTargetEntityUri = "urn:ngsi-ld:Entity:4567".toUri()
    private val sizeExpandedName = "https://uri.etsi.org/ngsi-ld/size"

    @AfterEach
    fun cleanData() {
        entityRepository.deleteAll()
        propertyRepository.deleteAll()
        relationshipRepository.deleteAll()
    }

    @Test
    fun `it should merge a partial entity to a normal entity`() {
        val partialEntity = PartialEntity(beekeeperUri)
        partialEntityRepository.save(partialEntity)
        createEntity(beekeeperUri, listOf("Beekeeper"))

        neo4jRepository.mergePartialWithNormalEntity(beekeeperUri)

        assertFalse(partialEntityRepository.existsById(beekeeperUri))
        assertTrue(entityRepository.existsById(beekeeperUri))
        val beekeeper = entityRepository.findById(beekeeperUri)
        assertEquals(listOf("Beekeeper"), beekeeper.get().types)
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
    }

    @Test
    fun `it should create an entity with a property whose name contains a colon`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(
                Property(name = "prefix:name", value = "value")
            )
        )

        val persistedEntity = entityRepository.findById(entity.id)
        assertTrue(persistedEntity.isPresent)
        val persistedProperty = persistedEntity.get().properties[0]
        assertEquals("prefix:name", persistedProperty.name)
    }

    @Test
    fun `it should create an entity with a property whose value is a JSON object`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(
                Property(
                    name = sizeExpandedName,
                    value = mapOf("length" to 2, "height" to 12, "comment" to "none")
                )
            )
        )

        val persistedEntity = entityRepository.findById(entity.id)
        assertTrue(persistedEntity.isPresent)
        val persistedProperty = persistedEntity.get().properties[0]
        assertEquals(mapOf("length" to 2, "height" to 12, "comment" to "none"), persistedProperty.value)
    }

    @Test
    fun `it should create a property for a subject`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper")
        )

        val property = Property(name = sizeExpandedName, value = 100L)

        val created = neo4jRepository.createPropertyOfSubject(EntitySubjectNode(entity.id), property)

        assertTrue(created)

        val propertyFromDb = propertyRepository.getPropertyOfSubject(entity.id, sizeExpandedName)
        assertEquals(sizeExpandedName, propertyFromDb.name)
        assertEquals(100L, propertyFromDb.value)
        assertNotNull(propertyFromDb.createdAt)
        assertNull(propertyFromDb.datasetId)
        assertNull(propertyFromDb.observedAt)
        assertEquals(0, propertyFromDb.properties.size)
        assertEquals(0, propertyFromDb.relationships.size)
    }

    @Test
    fun `it should create a property with a JSON object value for a sujbect`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper")
        )

        val propertyValue = mapOf("key1" to "value1", "key2" to 12, "key3" to listOf("v1", "v2"))
        val property = Property(name = sizeExpandedName, value = propertyValue)

        val created = neo4jRepository.createPropertyOfSubject(EntitySubjectNode(entity.id), property)

        assertTrue(created)

        val propertyFromDb = propertyRepository.getPropertyOfSubject(entity.id, sizeExpandedName)
        assertEquals(sizeExpandedName, propertyFromDb.name)
        assertEquals(propertyValue, propertyFromDb.value)
    }

    @Test
    fun `it should create a property with a list value for a subject`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper")
        )

        val propertyValue = listOf("v1", "v2")
        val property = Property(name = sizeExpandedName, value = propertyValue)

        val created = neo4jRepository.createPropertyOfSubject(EntitySubjectNode(entity.id), property)

        assertTrue(created)

        val propertyFromDb = propertyRepository.getPropertyOfSubject(entity.id, sizeExpandedName)
        assertEquals(sizeExpandedName, propertyFromDb.name)
        assertEquals(propertyValue, propertyFromDb.value)
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

        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should update the target relationship of a relationship`() {
        val property = createProperty("https://uri.etsi.org/ngsi-ld/temperature", 36)
        propertyRepository.save(property)
        val relationship = createRelationship(
            AttributeSubjectNode(property.id),
            EGM_OBSERVED_BY,
            "urn:ngsi-ld:Entity:123".toUri()
        )

        neo4jRepository.updateTargetOfRelationship(
            relationship.id,
            EGM_OBSERVED_BY.toRelationshipTypeName(),
            "urn:ngsi-ld:Entity:123".toUri(),
            "urn:ngsi-ld:Entity:456".toUri()
        )

        val updatedRelationship = relationshipRepository.findById(relationship.id)
        assertEquals("urn:ngsi-ld:Entity:456".toUri(), updatedRelationship.get().objectId)
    }

    @Test
    fun `it should add modifiedAt value when creating a new property`() {
        val property = Property(name = "name", value = "Scalpa")
        propertyRepository.save(property)
        assertNotNull(propertyRepository.findById(property.id).get().modifiedAt)
    }

    @Test
    fun `it should add modifiedAt value when saving a new relationship`() {
        val relationship = Relationship(objectId = "urn:ngsi-ld:Entity:target".toUri(), type = listOf("connectsTo"))
        relationshipRepository.save(relationship)
        assertNotNull(relationshipRepository.findById(relationship.id).get().modifiedAt)
    }

    @Test
    fun `it should update modifiedAt value when updating an entity`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val modifiedAt = entityRepository.findById("urn:ngsi-ld:Beekeeper:1233".toUri()).get().modifiedAt
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Demha"))
        )
        val updatedModifiedAt = entityRepository.findById("urn:ngsi-ld:Beekeeper:1233".toUri()).get().modifiedAt
        assertNotNull(updatedModifiedAt)
        assertThat(updatedModifiedAt).isAfter(modifiedAt)

        neo4jRepository.deleteEntity(entity.id)
        neo4jRepository.deleteEntity(secondEntity.id)
    }

    @Test
    fun `it should create createdAt property with time zone related information equal to the Z character`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        assertTrue(entity.createdAt.toString().endsWith("Z"))
    }

    @Test
    fun `it should create modifiedAt property with time zone related information equal to the Z character`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        assertTrue(entity.modifiedAt.toString().endsWith("Z"))
    }

    @Test
    fun `it should filter existing entityIds`() {
        createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )

        val entitiesIds = listOf("urn:ngsi-ld:Beekeeper:1233".toUri(), "urn:ngsi-ld:Beekeeper:1234".toUri())

        assertEquals(
            listOf("urn:ngsi-ld:Beekeeper:1233".toUri()),
            neo4jRepository.filterExistingEntitiesAsIds(entitiesIds)
        )
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

        assertEquals(1, entityRepository.findById(entity.id).get().properties.size)
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
    }

    @Test
    fun `it should delete incoming relationships when deleting an entity`() {
        val sensor = createEntity("urn:ngsi-ld:Sensor:1233".toUri(), listOf("Sensor"))
        val device = createEntity("urn:ngsi-ld:Device:1233".toUri(), listOf("Device"))
        createRelationship(EntitySubjectNode(device.id), EGM_OBSERVED_BY, sensor.id)

        neo4jRepository.deleteEntity(sensor.id)

        val entity = entityRepository.findById(device.id).get()
        assertEquals(0, entity.relationships.size)

        neo4jRepository.deleteEntity(sensor.id)
        neo4jRepository.deleteEntity(device.id)
    }

    @Test
    fun `it should return the default property instance if no datasetId is provided`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = 100L))
        )

        assertNotNull(propertyRepository.getPropertyOfSubject(entity.id, "name"))
    }

    @Test
    fun `it should not return a default property instance if a datasetId is provided`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = 100L, datasetId = "urn:ngsi-ld:Dataset:name:1".toUri()))
        )

        assertThrows<EmptyResultDataAccessException>("List is empty") {
            propertyRepository.getPropertyOfSubject(entity.id, "name")
        }
    }

    @Test
    fun `it should return the property instance with the given datasetId`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = 100L, datasetId = "urn:ngsi-ld:Dataset:name:1".toUri()))
        )

        assertNotNull(
            propertyRepository.getPropertyOfSubject(
                entity.id,
                "name",
                datasetId = "urn:ngsi-ld:Dataset:name:1".toUri()
            )
        )
    }

    @Test
    fun `it should not return the property instance if the given datasetId does not match`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = 100L, datasetId = "urn:ngsi-ld:Dataset:name:1".toUri()))
        )

        assertThrows<EmptyResultDataAccessException>("List is empty") {
            propertyRepository.getPropertyOfSubject(
                entity.id,
                "name",
                datasetId = "urn:ngsi-ld:Dataset:name:2".toUri()
            )
        }
    }

    @Test
    fun `it should return true if no datasetId is provided and a default instance exists`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = 100L))
        )

        assertTrue(neo4jRepository.hasPropertyInstance(EntitySubjectNode(entity.id), "name", null))
    }

    @Test
    fun `it should return false if no datasetId is provided and there is no default instance`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = 100L, datasetId = "urn:ngsi-ld:Dataset:name:1".toUri()))
        )

        assertFalse(neo4jRepository.hasPropertyInstance(EntitySubjectNode(entity.id), "name", null))
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
    }

    @Test
    fun `it should update the target of the default relationship instance of an entity`() {
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
            entityRepository.getRelationshipTargetOfSubject(sensor.id, EGM_OBSERVED_BY.toRelationshipTypeName())!!.id,
            newDevice.id
        )
    }

    @Test
    fun `it should update the target of a relationship instance with datasetId of an entity`() {
        val sensor = createEntity("urn:ngsi-ld:Sensor:1233".toUri(), listOf("Sensor"), mutableListOf())
        val device = createEntity("urn:ngsi-ld:Device:1233".toUri(), listOf("Device"), mutableListOf())
        val newDevice = createEntity("urn:ngsi-ld:Device:9978".toUri(), listOf("Device"), mutableListOf())
        val datasetId = "urn:ngsi-ld:Dataset:observedBy:device1".toUri()
        createRelationship(
            EntitySubjectNode(sensor.id),
            EGM_OBSERVED_BY,
            device.id,
            datasetId
        )

        neo4jRepository.updateRelationshipTargetOfSubject(
            sensor.id,
            EGM_OBSERVED_BY.toRelationshipTypeName(),
            newDevice.id,
            datasetId
        )

        assertEquals(
            entityRepository.getRelationshipTargetOfSubject(
                sensor.id, EGM_OBSERVED_BY.toRelationshipTypeName(), datasetId
            )!!.id,
            newDevice.id
        )
    }

    @Test
    fun `it should return an emptyMap for unknown type`() {
        createEntity(
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
    }

    @Test
    fun `it should retrieve entity type attributes information for two entities`() {
        createEntity(
            "urn:ngsi-ld:Beehive:TESTC".toUri(),
            listOf("https://ontology.eglobalmark.com/apic#Beehive"),
            mutableListOf(
                Property(name = "temperature", value = 36),
                Property(name = "humidity", value = 65)
            )
        )
        createEntity(
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

        assertEquals(3, propertiesInformation.size, "Got the following attributes: $propertiesInformation")
        assertTrue(propertiesInformation.containsAll(listOf("humidity", "temperature", "incoming")))
        assertEquals(attributesInformation["relationships"], emptySet<String>())
        assertEquals(attributesInformation["geoProperties"], emptyList<String>())
        assertEquals(attributesInformation["entityCount"], 2)
    }

    @Test
    fun `it should retrieve entity type attributes information for two entities with relationships`() {
        createEntity(
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
        assertEquals(attributesInformation["geoProperties"], emptyList<String>())
        assertEquals(attributesInformation["entityCount"], 2)
    }

    @Test
    fun `it should retrieve entity type attributes information for three entities with one geo property`() {
        createEntity(
            "urn:ngsi-ld:Beehive:TESTC".toUri(),
            listOf("https://ontology.eglobalmark.com/apic#Beehive"),
            mutableListOf(
                Property(name = "temperature", value = 36),
                Property(name = "humidity", value = 65)
            ),
            location = "POINT (24.30623 60.07966)"
        )
        createEntity(
            "urn:ngsi-ld:Beehive:TESTB".toUri(),
            listOf("https://ontology.eglobalmark.com/apic#Beehive"),
            mutableListOf(
                Property(name = "temperature", value = 30),
                Property(name = "humidity", value = 61)
            )
        )
        createEntity(
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
        assertEquals(
            attributesInformation["geoProperties"],
            listOf(
                "https://uri.etsi.org/ngsi-ld/location"
            )
        )
        assertEquals(attributesInformation["entityCount"], 3)
    }

    @Test
    fun `it should retrieve entity type attributes information for three entities with all geo properties`() {
        createEntity(
            "urn:ngsi-ld:Beehive:TESTC".toUri(),
            listOf("https://ontology.eglobalmark.com/apic#Beehive"),
            mutableListOf(
                Property(name = "temperature", value = 36),
                Property(name = "humidity", value = 65)
            ),
            location = "POINT (24.30623 60.07966)",
            operationSpace = "POINT (24.30623 60.07966)"
        )
        createEntity(
            "urn:ngsi-ld:Beehive:TESTB".toUri(),
            listOf("https://ontology.eglobalmark.com/apic#Beehive"),
            mutableListOf(
                Property(name = "temperature", value = 30),
                Property(name = "humidity", value = 61)
            )
        )
        createEntity(
            "urn:ngsi-ld:Beehive:TESTD".toUri(),
            listOf("https://ontology.eglobalmark.com/apic#Beehive"),
            mutableListOf(
                Property(name = "temperature", value = 25),
                Property(name = "humidity", value = 89),
            ),
            observationSpace = "POINT (24.30623 60.07966)"
        )

        val attributesInformation = neo4jRepository
            .getEntityTypeAttributesInformation("https://ontology.eglobalmark.com/apic#Beehive")

        val propertiesInformation = attributesInformation["properties"] as Set<*>

        assertEquals(propertiesInformation.size, 2)
        assertTrue(propertiesInformation.containsAll(listOf("humidity", "temperature")))
        assertEquals(attributesInformation["relationships"], emptySet<String>())
        assertEquals(
            attributesInformation["geoProperties"],
            listOf(
                "https://uri.etsi.org/ngsi-ld/location",
                "https://uri.etsi.org/ngsi-ld/operationSpace",
                "https://uri.etsi.org/ngsi-ld/observationSpace"
            )
        )
        assertEquals(attributesInformation["entityCount"], 3)
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
        createEntity(
            "urn:ngsi-ld:Sensor:TESTB".toUri(),
            listOf("https://ontology.eglobalmark.com/apic#Sensor"),
            mutableListOf(
                Property(name = "deviceParameter", value = 30),
                Property(name = "isContainedIn", value = 61)
            )
        )

        createRelationship(EntitySubjectNode(firstEntity.id), "observedBy", secondEntity.id)

        val entityTypesNames = neo4jRepository.getEntityTypesNames()

        assertEquals(2, entityTypesNames.size)
        assertTrue(
            entityTypesNames.containsAll(
                listOf("https://ontology.eglobalmark.com/apic#Beehive", "https://ontology.eglobalmark.com/apic#Sensor")
            )
        )
    }

    @Test
    fun `it should retrieve a list of entity types with all geo properties`() {
        val firstEntity = createEntity(
            "urn:ngsi-ld:Beehive:TESTC".toUri(),
            listOf("https://ontology.eglobalmark.com/apic#Beehive"),
            mutableListOf(
                Property(name = "temperature", value = 36),
                Property(name = "humidity", value = 65)
            )
        )
        createEntity(
            "urn:ngsi-ld:Beehive:TESTB".toUri(),
            listOf("https://ontology.eglobalmark.com/apic#Beehive"),
            mutableListOf(
                Property(name = "temperature", value = 36),
                Property(name = "name", value = "Beehive TESTB")
            ),
            location = "POINT (24.30623 60.07966)",
            operationSpace = "POINT (24.30623 60.07966)",
            observationSpace = "POINT (24.30623 60.07966)"

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

        assertEquals(2, entityTypes.size, "Got the following types instead: $entityTypes")
        assertTrue(
            entityTypes.containsAll(
                listOf(
                    mapOf(
                        "entityType" to "https://ontology.eglobalmark.com/apic#Beehive",
                        "properties" to setOf("temperature", "humidity", "name"),
                        "relationships" to setOf("observedBy"),
                        "geoProperties" to listOf(
                            "https://uri.etsi.org/ngsi-ld/location",
                            "https://uri.etsi.org/ngsi-ld/operationSpace",
                            "https://uri.etsi.org/ngsi-ld/observationSpace"
                        )
                    ),
                    mapOf(
                        "entityType" to "https://ontology.eglobalmark.com/apic#Sensor",
                        "properties" to setOf("deviceParameter"),
                        "relationships" to emptySet<String>(),
                        "geoProperties" to emptyList<String>()
                    )
                )
            )
        )
    }

    @Test
    fun `it should retrieve attribute list with all geo properties`() {
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
            ),
            location = "POINT (24.30623 60.07966)",
            operationSpace = "POINT (24.30623 60.07966)",
            observationSpace = "POINT (24.30623 60.07966)"
        )

        createRelationship(EntitySubjectNode(firstEntity.id), "observedBy", secondEntity.id)

        val attributeName = neo4jRepository.getAttributes()
        val expectedValue = listOf(
            "deviceParameter",
            "https://uri.etsi.org/ngsi-ld/location",
            "https://uri.etsi.org/ngsi-ld/observationSpace",
            "https://uri.etsi.org/ngsi-ld/operationSpace",
            "humidity",
            "isContainedIn",
            "observedBy",
            "temperature"
        )
        assertEquals(8, attributeName.size)
        assertArrayEquals(arrayOf(expectedValue), arrayOf(attributeName))
    }

    @Test
    fun `it should retrieve details of attributes with one geo property`() {
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
                Property(name = "temperature", value = 36),
                Property(name = "isContainedIn", value = 61)
            ),
            location = "POINT (24.30623 60.07966)"
        )
        val thirdEntity = createEntity(
            "urn:ngsi-ld:Beehive:TESTB".toUri(),
            listOf("https://ontology.eglobalmark.com/apic#Beehive"),
            mutableListOf(Property(name = "humidity", value = 65))
        )

        createRelationship(EntitySubjectNode(firstEntity.id), "observedBy", secondEntity.id)
        createRelationship(EntitySubjectNode(secondEntity.id), "observedBy", thirdEntity.id)

        val attributes = neo4jRepository.getAttributesDetails()
        assertEquals(6, attributes.size, "Got the following types instead: $attributes")
        assertArrayEquals(
            arrayOf(
                mapOf(
                    "attribute" to "deviceParameter",
                    "typeNames" to setOf("https://ontology.eglobalmark.com/apic#Sensor")
                ),
                mapOf(
                    "attribute" to "https://uri.etsi.org/ngsi-ld/location",
                    "typeNames" to setOf("https://ontology.eglobalmark.com/apic#Sensor")
                ),
                mapOf(
                    "attribute" to "humidity",
                    "typeNames" to setOf("https://ontology.eglobalmark.com/apic#Beehive")
                ),
                mapOf(
                    "attribute" to "isContainedIn",
                    "typeNames" to setOf("https://ontology.eglobalmark.com/apic#Sensor")
                ),
                mapOf(
                    "attribute" to "observedBy",
                    "typeNames" to setOf(
                        "https://ontology.eglobalmark.com/apic#Beehive",
                        "https://ontology.eglobalmark.com/apic#Sensor"
                    )
                ),
                mapOf(
                    "attribute" to "temperature",
                    "typeNames" to setOf(
                        "https://ontology.eglobalmark.com/apic#Beehive",
                        "https://ontology.eglobalmark.com/apic#Sensor"
                    )
                )
            ),
            attributes.toTypedArray()
        )
    }

    @Test
    fun `it should return an empty result if attribute is unknown`() {
        createEntity(
            "urn:ngsi-ld:Beehive:TESTC".toUri(),
            listOf("https://ontology.eglobalmark.com/apic#Beehive"),
            mutableListOf(
                Property(name = "temperature", value = 36),
                Property(name = "humidity", value = 65)
            )
        )

        val attributesInformation = neo4jRepository.getAttributeInformation("unknown")

        assertTrue(attributesInformation.isEmpty())
    }

    @Test
    fun `it should retrieve attribute information for two entities if attribute is a relationship type`() {
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
            listOf("https://ontology.eglobalmark.com/apic#Sensor"),
            mutableListOf(
                Property(name = "temperature", value = 30),
                Property(name = "humidity", value = 61)
            )
        )
        createRelationship(EntitySubjectNode(secondEntity.id), "observedBy", partialTargetEntityUri)
        createRelationship(EntitySubjectNode(firstEntity.id), "observedBy", secondEntity.id)

        val actualAttributesInformation = neo4jRepository.getAttributeInformation("observedBy")

        val expectedAttributesInformation = mapOf(
            "attributeName" to "observedBy",
            "attributeTypes" to setOf("Relationship"),
            "typeNames" to setOf(
                "https://ontology.eglobalmark.com/apic#Beehive",
                "https://ontology.eglobalmark.com/apic#Sensor"
            ),
            "attributeCount" to 2
        )

        assertEquals(4, actualAttributesInformation.size, "Got the following attributes: $actualAttributesInformation")
        assertArrayEquals(arrayOf(expectedAttributesInformation), arrayOf(actualAttributesInformation))
    }

    @Test
    fun `it should retrieve attribute information for two entities if attribute is a property type`() {
        createEntity(
            "urn:ngsi-ld:Beehive:TESTC".toUri(),
            listOf("https://ontology.eglobalmark.com/apic#Beehive"),
            mutableListOf(
                Property(name = "temperature", value = 36),
                Property(name = "humidity", value = 65)
            )
        )
        createEntity(
            "urn:ngsi-ld:Sensor:TESTC".toUri(),
            listOf("https://ontology.eglobalmark.com/apic#Sensor"),
            mutableListOf(
                Property(name = "deviceParameter", value = 30),
                Property(name = "humidity", value = 60)
            )
        )

        val actualAttributesInformation = neo4jRepository.getAttributeInformation("humidity")

        val expectedAttributesInformation = mapOf(
            "attributeName" to "humidity",
            "attributeTypes" to setOf("Property"),
            "typeNames" to setOf(
                "https://ontology.eglobalmark.com/apic#Beehive",
                "https://ontology.eglobalmark.com/apic#Sensor"
            ),
            "attributeCount" to 2
        )

        assertEquals(4, actualAttributesInformation.size, "Got the following attributes: $actualAttributesInformation")
        assertArrayEquals(arrayOf(expectedAttributesInformation), arrayOf(actualAttributesInformation))
    }

    @Test
    fun `it should return the relationship instance with datasetId of an entity`() {
        val sensor = createEntity("urn:ngsi-ld:Sensor:1233".toUri(), listOf("Sensor"), mutableListOf())
        val device = createEntity("urn:ngsi-ld:Device:1233".toUri(), listOf("Device"), mutableListOf())
        createRelationship(
            EntitySubjectNode(sensor.id),
            EGM_OBSERVED_BY,
            device.id,
            "urn:ngsi-ld:Dataset:observedBy:01".toUri()
        )

        val relationship = relationshipRepository.getRelationshipOfSubject(
            sensor.id,
            EGM_OBSERVED_BY.toRelationshipTypeName(),
            "urn:ngsi-ld:Dataset:observedBy:01".toUri()
        )

        assertEquals(relationship.type, listOf(EGM_OBSERVED_BY))
    }

    @Test
    fun `has geo property of name should return true`() {
        val entity = createEntity(
            "urn:ngsi-ld:Sensor:1233".toUri(),
            listOf("Sensor"),
            location = "POINT (24.30623 60.07966)"
        )

        assertTrue(
            neo4jRepository.hasGeoPropertyOfName(EntitySubjectNode("urn:ngsi-ld:Sensor:1233".toUri()), "location")
        )

        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `has geo property of name should return false`() {
        val entity = createEntity(
            "urn:ngsi-ld:Sensor:1233".toUri(),
            listOf("Sensor")
        )

        assertFalse(
            neo4jRepository.hasGeoPropertyOfName(EntitySubjectNode("urn:ngsi-ld:Sensor:1233".toUri()), "location")
        )

        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should update geo property`() {
        val entity = createEntity(
            "urn:ngsi-ld:Sensor:1233".toUri(),
            listOf("Sensor"),
            location = "POINT (24.30623 60.07966)"
        )

        val geoProperty = NgsiLdGeoPropertyInstance(
            coordinates = WKTCoordinates("POINT (25.30623 61.07966)"),
            createdAt = null,
            modifiedAt = null
        )

        assertEquals(
            1,
            neo4jRepository.updateGeoPropertyOfEntity("urn:ngsi-ld:Sensor:1233".toUri(), "location", geoProperty)
        )

        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should add new geo property`() {
        val entity = createEntity(
            "urn:ngsi-ld:Sensor:1233".toUri(),
            listOf("Sensor")
        )

        val geoProperty = NgsiLdGeoPropertyInstance(
            coordinates = WKTCoordinates("POINT (24.30623 60.07966)"),
            createdAt = null,
            modifiedAt = null
        )

        assertFalse(
            neo4jRepository.hasGeoPropertyOfName(EntitySubjectNode("urn:ngsi-ld:Sensor:1233".toUri()), "location")
        )

        neo4jRepository.addGeoPropertyToEntity("urn:ngsi-ld:Sensor:1233".toUri(), "location", geoProperty)

        assertTrue(
            neo4jRepository.hasGeoPropertyOfName(EntitySubjectNode("urn:ngsi-ld:Sensor:1233".toUri()), "location")
        )

        neo4jRepository.deleteEntity(entity.id)
    }

    fun createEntity(
        id: URI,
        types: List<String>,
        properties: MutableList<Property> = mutableListOf(),
        location: String? = null,
        operationSpace: String? = null,
        observationSpace: String? = null
    ): Entity {
        val entity = Entity(
            id = id,
            types = types,
            properties = properties,
            location = location,
            operationSpace = operationSpace,
            observationSpace = observationSpace
        )
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
        val relationship = Relationship(objectId = objectId, type = listOf(relationshipType), datasetId = datasetId)
        neo4jRepository.createRelationshipOfSubject(subjectNodeInfo, relationship, objectId)

        return relationship
    }
}
