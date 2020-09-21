package com.egm.stellio.entity.repository

import com.egm.stellio.entity.config.TestContainersConfiguration
import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.entity.model.toRelationshipTypeName
import com.egm.stellio.shared.model.NgsiLdProperty
import com.egm.stellio.shared.model.parseToNgsiLdAttributes
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.EGM_IS_CONTAINED_IN
import com.egm.stellio.shared.util.JsonLdUtils.EGM_OBSERVED_BY
import com.egm.stellio.shared.util.JsonLdUtils.EGM_VENDOR_ID
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

    @Test
    fun `it should return an entity if type and string properties are correct`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1230".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val entities: List<URI> = neo4jRepository.getEntitiesByTypeAndQuery(
            "Beekeeper",
            Pair(listOf(), listOf(Triple("name", "=", "Scalpa")))
        )
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an empty list if string properties are wrong`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val entities: List<URI> = neo4jRepository.getEntitiesByTypeAndQuery(
            "Beekeeper",
            Pair(listOf(), listOf(Triple("name", "=", "ScalpaXYZ")))
        )
        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if type and integer properties are correct`() {
        val entity = createEntity(
            "urn:ngsi-ld:DeadFishes:019BN".toUri(),
            listOf("DeadFishes"),
            mutableListOf(Property(name = "fishNumber", value = 500))
        )
        val entities: List<URI> = neo4jRepository.getEntitiesByTypeAndQuery(
            "DeadFishes",
            Pair(listOf(), listOf(Triple("fishNumber", "=", "500")))
        )
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an empty list if integer properties are wrong`() {
        val entity = createEntity(
            "urn:ngsi-ld:DeadFishes:019BO".toUri(),
            listOf("DeadFishes"),
            mutableListOf(Property(name = "fishNumber", value = 500))
        )
        val entities: List<URI> = neo4jRepository.getEntitiesByTypeAndQuery(
            "DeadFishes",
            Pair(listOf(), listOf(Triple("fishNumber", "=", "499")))
        )
        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if type and float properties are correct`() {
        val entity = createEntity(
            "urn:ngsi-ld:DeadFishes:019BP".toUri(),
            listOf("DeadFishes"),
            mutableListOf(Property(name = "fishWeight", value = 120.50))
        )
        val entities: List<URI> = neo4jRepository.getEntitiesByTypeAndQuery(
            "DeadFishes",
            Pair(listOf(), listOf(Triple("fishWeight", "=", "120.50")))
        )
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an empty list if float properties are wrong`() {
        val entity = createEntity(
            "urn:ngsi-ld:DeadFishes:019BQ".toUri(),
            listOf("DeadFishes"),
            mutableListOf(Property(name = "fishWeight", value = -120.50))
        )
        val entities: List<URI> = neo4jRepository.getEntitiesByTypeAndQuery(
            "DeadFishes",
            Pair(listOf(), listOf(Triple("fishWeight", "=", "-120")))
        )
        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an empty list if given weight equals to entity weight and comparaison parameter is wrong`() {
        val entity = createEntity(
            "urn:ngsi-ld:DeadFishes:019BR".toUri(),
            listOf("DeadFishes"),
            mutableListOf(Property(name = "fishWeight", value = 180.9))
        )
        val entities: List<URI> = neo4jRepository.getEntitiesByTypeAndQuery(
            "DeadFishes",
            Pair(listOf(), listOf(Triple("fishWeight", ">", "180.9")))
        )
        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if given weight equals to entity weight and comparaison parameter is correct`() {
        val entity = createEntity(
            "urn:ngsi-ld:DeadFishes:019BS".toUri(),
            listOf("DeadFishes"),
            mutableListOf(Property(name = "fishWeight", value = 255))
        )
        val entities: List<URI> = neo4jRepository.getEntitiesByTypeAndQuery(
            "DeadFishes",
            Pair(listOf(), listOf(Triple("fishWeight", ">=", "255")))
        )
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an empty list if given name equals to entity name and comparaison parameter is wrong`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1232".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "ScalpaXYZ"))
        )
        val entities: List<URI> = neo4jRepository.getEntitiesByTypeAndQuery(
            "Beekeeper",
            Pair(listOf(), listOf(Triple("name", "<>", "ScalpaXYZ")))
        )
        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if given name not equals to entity name and comparaison parameter is correct`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val entities = neo4jRepository.getEntitiesByTypeAndQuery(
            "Beekeeper",
            Pair(listOf(), listOf(Triple("name", "<>", "ScalpaXYZ")))
        )
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if type and dateTime properties are correct`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1234".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "testedAt", value = ZonedDateTime.parse("2018-12-04T12:00:00Z")))
        )
        val entities = neo4jRepository.getEntitiesByTypeAndQuery(
            "Beekeeper",
            Pair(listOf(), listOf(Triple("testedAt", "=", "2018-12-04T12:00:00Z")))
        )
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if type and date properties are correct`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1235".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "testedAt", value = LocalDate.parse("2018-12-04")))
        )
        val entities = neo4jRepository.getEntitiesByTypeAndQuery(
            "Beekeeper",
            Pair(listOf(), listOf(Triple("testedAt", "=", "2018-12-04")))
        )
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should not return an entity if type is correct but not the compared date`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1235".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "testedAt", value = LocalDate.parse("2018-12-04")))
        )
        val entities = neo4jRepository.getEntitiesByTypeAndQuery(
            "Beekeeper",
            Pair(listOf(), listOf(Triple("testedAt", "=", "2018-12-07")))
        )
        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if type and time properties are correct`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1236".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "testedAt", value = LocalTime.parse("12:00:00")))
        )
        val entities: List<URI> = neo4jRepository.getEntitiesByTypeAndQuery(
            "Beekeeper",
            Pair(listOf(), listOf(Triple("testedAt", "=", "12:00:00")))
        )
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should update the default property instance`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
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
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
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
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
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
    fun `it should update relationships of a property instance`() {
        val temperatureProperty = createProperty("https://uri.etsi.org/ngsi-ld/temperature", 36)
        propertyRepository.save(temperatureProperty)

        val targetEntity = createEntity(
            "urn:ngsi-ld:Sensor:1233".toUri(),
            listOf("Sensor"),
            mutableListOf()
        )
        createRelationship(AttributeSubjectNode(temperatureProperty.id), EGM_OBSERVED_BY, targetEntity.id)
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(temperatureProperty)
        )

        createEntity("urn:ngsi-ld:Sensor:6789".toUri(), listOf("Sensor"), mutableListOf())
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
    fun `it should update a property value of a string type`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        neo4jRepository.updateEntityAttribute(entity.id, "name", "new name")
        assertEquals("new name", neo4jRepository.getPropertyOfSubject(entity.id, "name").value)
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should update a property value of a numeric type`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = 100L))
        )
        neo4jRepository.updateEntityAttribute(entity.id, "name", 200L)
        assertEquals(200L, neo4jRepository.getPropertyOfSubject(entity.id, "name").value)
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
    fun `it should find a sensor by vendor id and measured property`() {
        val vendorId = "urn:something:9876".toUri()
        val entity = createEntity(
            "urn:ngsi-ld:Sensor:1233".toUri(),
            listOf("Sensor"),
            mutableListOf(Property(name = EGM_VENDOR_ID, value = vendorId))
        )
        val property = createProperty("https://ontology.eglobalmark.com/apic#outgoing", 1.0)
        val relationship = createRelationship(AttributeSubjectNode(property.id), EGM_OBSERVED_BY, entity.id)
        val persistedEntity = neo4jRepository.getObservingSensorEntity(vendorId, EGM_VENDOR_ID, "outgoing")
        assertNotNull(persistedEntity)
        propertyRepository.delete(property)
        relationshipRepository.delete(relationship)
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should find a sensor by vendor id with case not matching and measured property`() {
        val vendorId = "urn:something:9876".toUri()
        val entity = createEntity(
            "urn:ngsi-ld:Sensor:1233".toUri(),
            listOf("Sensor"),
            mutableListOf(Property(name = EGM_VENDOR_ID, value = vendorId))
        )
        val property = createProperty("https://ontology.eglobalmark.com/apic#outgoing", 1.0)
        val relationship = createRelationship(AttributeSubjectNode(property.id), EGM_OBSERVED_BY, entity.id)
        val persistedEntity =
            neo4jRepository.getObservingSensorEntity(
                vendorId.toString().toUpperCase().toUri(),
                EGM_VENDOR_ID,
                "outgoing"
            )
        assertNotNull(persistedEntity)
        propertyRepository.delete(property)
        relationshipRepository.delete(relationship)
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should find a sensor by vendor id of a device and measured property`() {
        val vendorId = "urn:something:9876".toUri()
        val sensor = createEntity("urn:ngsi-ld:Sensor:1233".toUri(), listOf("Sensor"), mutableListOf())
        val device = createEntity(
            "urn:ngsi-ld:Device:1233".toUri(),
            listOf("Device"),
            mutableListOf(Property(name = EGM_VENDOR_ID, value = vendorId))
        )
        val sensorToDeviceRelationship =
            createRelationship(EntitySubjectNode(sensor.id), EGM_IS_CONTAINED_IN, device.id)
        val property = createProperty("https://ontology.eglobalmark.com/apic#outgoing", 1.0)
        val propertyToDeviceRelationship =
            createRelationship(AttributeSubjectNode(property.id), EGM_OBSERVED_BY, sensor.id)
        val persistedEntity = neo4jRepository.getObservingSensorEntity(vendorId, EGM_VENDOR_ID, "outgoing")
        assertNotNull(persistedEntity)
        propertyRepository.delete(property)
        relationshipRepository.delete(sensorToDeviceRelationship)
        relationshipRepository.delete(propertyToDeviceRelationship)
        neo4jRepository.deleteEntity(device.id)
        neo4jRepository.deleteEntity(sensor.id)
    }

    @Test
    fun `it should not find a sensor by vendor id if measured property does not exist`() {
        val vendorId = "urn:something:9876".toUri()
        val entity = createEntity(
            "urn:ngsi-ld:Sensor:1233".toUri(),
            listOf("Sensor"),
            mutableListOf(Property(name = EGM_VENDOR_ID, value = vendorId))
        )
        val property = createProperty("https://ontology.eglobalmark.com/apic#outgoing", 1.0)
        val relationship = createRelationship(AttributeSubjectNode(property.id), EGM_OBSERVED_BY, entity.id)
        val persistedEntity = neo4jRepository.getObservingSensorEntity(vendorId, EGM_VENDOR_ID, "incoming")
        assertNull(persistedEntity)
        propertyRepository.delete(property)
        relationshipRepository.delete(relationship)
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should not find a sensor if measured property exists but not the vendor id`() {
        val vendorId = "urn:something:9876".toUri()
        val entity = createEntity(
            "urn:ngsi-ld:Sensor:1233".toUri(),
            listOf("Sensor"),
            mutableListOf(Property(name = EGM_VENDOR_ID, value = vendorId))
        )
        val property = createProperty("https://ontology.eglobalmark.com/apic#outgoing", 1.0)
        val relationship = createRelationship(AttributeSubjectNode(property.id), EGM_OBSERVED_BY, entity.id)
        val persistedEntity =
            neo4jRepository.getObservingSensorEntity(
                "urn:ngsi-ld:Sensor:Unknown".toUri(),
                EGM_VENDOR_ID,
                "outgoing"
            )
        assertNull(persistedEntity)
        propertyRepository.delete(property)
        relationshipRepository.delete(relationship)
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should find a sensor by NGSI-LD id`() {
        val entity = createEntity("urn:ngsi-ld:Sensor:1233".toUri(), listOf("Sensor"), mutableListOf())
        val persistedEntity =
            neo4jRepository.getObservingSensorEntity(
                "urn:ngsi-ld:Sensor:1233".toUri(),
                EGM_VENDOR_ID,
                "anything"
            )
        assertNotNull(persistedEntity)
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should not find a sensor if neither NGSI-LD or vendor id matches`() {
        val entity = createEntity(
            "urn:ngsi-ld:Sensor:1233".toUri(),
            listOf("Sensor"),
            mutableListOf(Property(name = EGM_VENDOR_ID, value = "urn:something:9876"))
        )
        val persistedEntity =
            neo4jRepository.getObservingSensorEntity(
                "urn:ngsi-ld:Sensor:Unknown".toUri(),
                EGM_VENDOR_ID,
                "unknownMeasure"
            )
        assertNull(persistedEntity)
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
        val sensor = createEntity("urn:ngsi-ld:Sensor:1233".toUri(), listOf("Sensor"), mutableListOf())
        val device = createEntity("urn:ngsi-ld:Device:1233".toUri(), listOf("Device"), mutableListOf())
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
        val sensor = createEntity("urn:ngsi-ld:Sensor:1233".toUri(), listOf("Sensor"), mutableListOf())
        val device = createEntity("urn:ngsi-ld:Device:1233".toUri(), listOf("Device"), mutableListOf())
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
        val sensor = createEntity("urn:ngsi-ld:Sensor:1233".toUri(), listOf("Sensor"), mutableListOf())
        val device = createEntity("urn:ngsi-ld:Device:1233".toUri(), listOf("Device"), mutableListOf())
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
        val device = createEntity("urn:ngsi-ld:Device:1233".toUri(), listOf("Device"), mutableListOf())
        createRelationship(EntitySubjectNode(sensor.id), EGM_OBSERVED_BY, device.id)

        neo4jRepository.deleteEntityAttributes(sensor.id)

        val entity = entityRepository.findById(sensor.id).get()
        assertEquals(entity.relationships.size, 0)
        assertEquals(entity.properties.size, 0)

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
        val sensor = createEntity("urn:ngsi-ld:Sensor:1233".toUri(), listOf("Sensor"), mutableListOf())
        val device = createEntity("urn:ngsi-ld:Device:1233".toUri(), listOf("Device"), mutableListOf())
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
        val sensor = createEntity("urn:ngsi-ld:Sensor:1233".toUri(), listOf("Sensor"), mutableListOf())
        val device = createEntity("urn:ngsi-ld:Device:1233".toUri(), listOf("Device"), mutableListOf())
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
        val sensor = createEntity("urn:ngsi-ld:Sensor:1233".toUri(), listOf("Sensor"), mutableListOf())
        val device = createEntity("urn:ngsi-ld:Device:1233".toUri(), listOf("Device"), mutableListOf())
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
        val sensor = createEntity("urn:ngsi-ld:Sensor:1233".toUri(), listOf("Sensor"), mutableListOf())
        val device = createEntity("urn:ngsi-ld:Device:1233".toUri(), listOf("Device"), mutableListOf())
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

    fun createEntity(id: URI, type: List<String>, properties: MutableList<Property>): Entity {
        val entity = Entity(id = id, type = type, properties = properties)
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
