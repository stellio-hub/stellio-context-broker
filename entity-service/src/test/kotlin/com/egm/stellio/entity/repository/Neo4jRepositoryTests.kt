package com.egm.stellio.entity.repository

import com.egm.stellio.entity.config.TestContainersConfiguration
import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.shared.util.NgsiLdParsingUtils.EGM_IS_CONTAINED_IN
import com.egm.stellio.shared.util.NgsiLdParsingUtils.EGM_OBSERVED_BY
import com.egm.stellio.shared.util.NgsiLdParsingUtils.EGM_VENDOR_ID
import junit.framework.TestCase.assertEquals
import junit.framework.TestCase.assertFalse
import junit.framework.TestCase.assertNotNull
import junit.framework.TestCase.assertNull
import junit.framework.TestCase.assertTrue
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.ActiveProfiles
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
            "urn:ngsi-ld:Beekeeper:1230",
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val entities: List<String> = neo4jRepository.getEntitiesByTypeAndQuery(
            "Beekeeper",
            Pair(listOf(), listOf(Triple("name", "=", "Scalpa")))
        )
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an empty list if string properties are wrong`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1231",
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val entities: List<String> = neo4jRepository.getEntitiesByTypeAndQuery(
            "Beekeeper",
            Pair(listOf(), listOf(Triple("name", "=", "ScalpaXYZ")))
        )
        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if type and integer properties are correct`() {
        val entity = createEntity(
            "urn:ngsi-ld:DeadFishes:019BN",
            listOf("DeadFishes"),
            mutableListOf(Property(name = "fishNumber", value = 500))
        )
        val entities: List<String> = neo4jRepository.getEntitiesByTypeAndQuery(
            "DeadFishes",
            Pair(listOf(), listOf(Triple("fishNumber", "=", "500")))
        )
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an empty list if integer properties are wrong`() {
        val entity = createEntity(
            "urn:ngsi-ld:DeadFishes:019BO",
            listOf("DeadFishes"),
            mutableListOf(Property(name = "fishNumber", value = 500))
        )
        val entities: List<String> = neo4jRepository.getEntitiesByTypeAndQuery(
            "DeadFishes",
            Pair(listOf(), listOf(Triple("fishNumber", "=", "499")))
        )
        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if type and float properties are correct`() {
        val entity = createEntity(
            "urn:ngsi-ld:DeadFishes:019BP",
            listOf("DeadFishes"),
            mutableListOf(Property(name = "fishWeight", value = 120.50))
        )
        val entities: List<String> = neo4jRepository.getEntitiesByTypeAndQuery(
            "DeadFishes",
            Pair(listOf(), listOf(Triple("fishWeight", "=", "120.50")))
        )
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an empty list if float properties are wrong`() {
        val entity = createEntity(
            "urn:ngsi-ld:DeadFishes:019BQ",
            listOf("DeadFishes"),
            mutableListOf(Property(name = "fishWeight", value = -120.50))
        )
        val entities: List<String> = neo4jRepository.getEntitiesByTypeAndQuery(
            "DeadFishes",
            Pair(listOf(), listOf(Triple("fishWeight", "=", "-120")))
        )
        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an empty list if given weight equals to entity weight and comparaison parameter is wrong`() {
        val entity = createEntity(
            "urn:ngsi-ld:DeadFishes:019BR",
            listOf("DeadFishes"),
            mutableListOf(Property(name = "fishWeight", value = 180.9))
        )
        val entities: List<String> = neo4jRepository.getEntitiesByTypeAndQuery(
            "DeadFishes",
            Pair(listOf(), listOf(Triple("fishWeight", ">", "180.9")))
        )
        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if given weight equals to entity weight and comparaison parameter is correct`() {
        val entity = createEntity(
            "urn:ngsi-ld:DeadFishes:019BS",
            listOf("DeadFishes"),
            mutableListOf(Property(name = "fishWeight", value = 255))
        )
        val entities: List<String> = neo4jRepository.getEntitiesByTypeAndQuery(
            "DeadFishes",
            Pair(listOf(), listOf(Triple("fishWeight", ">=", "255")))
        )
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an empty list if given name equals to entity name and comparaison parameter is wrong`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1232",
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "ScalpaXYZ"))
        )
        val entities: List<String> = neo4jRepository.getEntitiesByTypeAndQuery(
            "Beekeeper",
            Pair(listOf(), listOf(Triple("name", "<>", "ScalpaXYZ")))
        )
        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if given name not equals to entity name and comparaison parameter is correct`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233",
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
            "urn:ngsi-ld:Beekeeper:1234",
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
            "urn:ngsi-ld:Beekeeper:1235",
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
            "urn:ngsi-ld:Beekeeper:1235",
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
            "urn:ngsi-ld:Beekeeper:1236",
            listOf("Beekeeper"),
            mutableListOf(Property(name = "testedAt", value = LocalTime.parse("12:00:00")))
        )
        val entities: List<String> = neo4jRepository.getEntitiesByTypeAndQuery(
            "Beekeeper",
            Pair(listOf(), listOf(Triple("testedAt", "=", "12:00:00")))
        )
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should update a property value of a string type`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233",
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
            "urn:ngsi-ld:Beekeeper:1233",
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
            "urn:ngsi-ld:Beekeeper:1233",
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val modifiedAt = entityRepository.findById("urn:ngsi-ld:Beekeeper:1233").get().modifiedAt
        createEntity(
            "urn:ngsi-ld:Beekeeper:1233",
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Demha"))
        )
        val updatedModifiedAt = entityRepository.findById("urn:ngsi-ld:Beekeeper:1233").get().modifiedAt
        assertThat(updatedModifiedAt).isAfter(modifiedAt)
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should find a sensor by vendor id and measured property`() {
        val vendorId = "urn:something:9876"
        val entity = createEntity(
            "urn:ngsi-ld:Sensor:1233",
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
        val vendorId = "urn:something:9876"
        val entity = createEntity(
            "urn:ngsi-ld:Sensor:1233",
            listOf("Sensor"),
            mutableListOf(Property(name = EGM_VENDOR_ID, value = vendorId))
        )
        val property = createProperty("https://ontology.eglobalmark.com/apic#outgoing", 1.0)
        val relationship = createRelationship(AttributeSubjectNode(property.id), EGM_OBSERVED_BY, entity.id)
        val persistedEntity =
            neo4jRepository.getObservingSensorEntity(vendorId.toUpperCase(), EGM_VENDOR_ID, "outgoing")
        assertNotNull(persistedEntity)
        propertyRepository.delete(property)
        relationshipRepository.delete(relationship)
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should find a sensor by vendor id of a device and measured property`() {
        val vendorId = "urn:something:9876"
        val sensor = createEntity("urn:ngsi-ld:Sensor:1233", listOf("Sensor"), mutableListOf())
        val device = createEntity(
            "urn:ngsi-ld:Device:1233",
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
        val vendorId = "urn:something:9876"
        val entity = createEntity(
            "urn:ngsi-ld:Sensor:1233",
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
        val vendorId = "urn:something:9876"
        val entity = createEntity(
            "urn:ngsi-ld:Sensor:1233",
            listOf("Sensor"),
            mutableListOf(Property(name = EGM_VENDOR_ID, value = vendorId))
        )
        val property = createProperty("https://ontology.eglobalmark.com/apic#outgoing", 1.0)
        val relationship = createRelationship(AttributeSubjectNode(property.id), EGM_OBSERVED_BY, entity.id)
        val persistedEntity =
            neo4jRepository.getObservingSensorEntity("urn:ngsi-ld:Sensor:Unknown", EGM_VENDOR_ID, "outgoing")
        assertNull(persistedEntity)
        propertyRepository.delete(property)
        relationshipRepository.delete(relationship)
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should find a sensor by NGSI-LD id`() {
        val entity = createEntity("urn:ngsi-ld:Sensor:1233", listOf("Sensor"), mutableListOf())
        val persistedEntity =
            neo4jRepository.getObservingSensorEntity("urn:ngsi-ld:Sensor:1233", EGM_VENDOR_ID, "anything")
        assertNotNull(persistedEntity)
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should not find a sensor if neither NGSI-LD or vendor id matches`() {
        val entity = createEntity(
            "urn:ngsi-ld:Sensor:1233",
            listOf("Sensor"),
            mutableListOf(Property(name = EGM_VENDOR_ID, value = "urn:something:9876"))
        )
        val persistedEntity =
            neo4jRepository.getObservingSensorEntity("urn:ngsi-ld:Sensor:Unknown", EGM_VENDOR_ID, "unknownMeasure")
        assertNull(persistedEntity)
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should create createdAt property with time zone related information equal to the character "Z"`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233",
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        assertTrue(entity.createdAt.toString().endsWith("Z"))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should create modifiedAt property with time zone related information equal to the character "Z"`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233",
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        assertTrue(entity.modifiedAt.toString().endsWith("Z"))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should filter existing entityIds`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233",
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )

        val entitiesIds = listOf("urn:ngsi-ld:Beekeeper:1233", "urn:ngsi-ld:Beekeeper:1234")

        assertEquals(
            listOf("urn:ngsi-ld:Beekeeper:1233"),
            neo4jRepository.filterExistingEntitiesIds(entitiesIds)
        )
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should delete an entity property`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233",
            listOf("Beekeeper"),
            mutableListOf(
                Property(name = "firstName", value = "Scalpa"),
                Property(name = "lastName", value = "Charity")
            )
        )

        neo4jRepository.deleteEntityProperty(entity.id, "lastName")

        assertEquals(entityRepository.findById(entity.id).get().properties.size, 1)
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should delete an entity relationship`() {
        val sensor = createEntity("urn:ngsi-ld:Sensor:1233", listOf("Sensor"), mutableListOf())
        val device = createEntity("urn:ngsi-ld:Device:1233", listOf("Device"), mutableListOf())
        createRelationship(EntitySubjectNode(sensor.id), EGM_OBSERVED_BY, device.id)

        neo4jRepository.deleteEntityRelationship(sensor.id, "OBSERVED_BY")

        assertEquals(entityRepository.findById(sensor.id).get().relationships.size, 0)

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
            "urn:ngsi-ld:Beekeeper:1233",
            listOf("Beekeeper"),
            mutableListOf(Property(name = "firstName", value = "Scalpa"), lastNameProperty)
        )

        neo4jRepository.deleteEntityProperty(entity.id, "lastName")

        assertTrue(propertyRepository.findById(originProperty.id).isEmpty)
        assertTrue(propertyRepository.findById(lastNameProperty.id).isEmpty)
        assertEquals(entityRepository.findById(entity.id).get().properties.size, 1)

        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should delete entity attributes`() {
        val sensor = createEntity(
            "urn:ngsi-ld:Sensor:1233",
            listOf("Sensor"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val device = createEntity("urn:ngsi-ld:Device:1233", listOf("Device"), mutableListOf())
        createRelationship(EntitySubjectNode(sensor.id), EGM_OBSERVED_BY, device.id)

        neo4jRepository.deleteEntityAttributes(sensor.id)

        val entity = entityRepository.findById(sensor.id).get()
        assertEquals(entity.relationships.size, 0)
        assertEquals(entity.properties.size, 0)

        neo4jRepository.deleteEntity(sensor.id)
        neo4jRepository.deleteEntity(device.id)
    }

    fun createEntity(id: String, type: List<String>, properties: MutableList<Property>): Entity {
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
        objectId: String
    ): Relationship {
        val relationship = Relationship(type = listOf(relationshipType))
        neo4jRepository.createRelationshipOfSubject(subjectNodeInfo, relationship, objectId)

        return relationship
    }
}
