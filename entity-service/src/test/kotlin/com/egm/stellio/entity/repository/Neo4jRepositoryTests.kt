package com.egm.stellio.entity.repository

import com.egm.stellio.entity.model.Attribute
import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.shared.util.NgsiLdParsingUtils.EGM_OBSERVED_BY
import com.egm.stellio.shared.util.NgsiLdParsingUtils.EGM_VENDOR_ID
import com.egm.stellio.shared.util.toRelationshipTypeName
import junit.framework.TestCase.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.beans.factory.annotation.Autowired

@SpringBootTest
@ActiveProfiles("test")
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
    private lateinit var attributeRepository: AttributeRepository

    @Test
    fun `it should return an entity if type and string properties are correct`() {
        val entity = createEntity("urn:ngsi-ld:Beekeeper:1230", listOf("Beekeeper"), mutableListOf(Property(name = "name", value = "Scalpa")))
        val entities: List<String> = neo4jRepository.getEntitiesByTypeAndQuery("Beekeeper", Pair(listOf(), listOf(Triple("name", "=", "Scalpa"))))
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an empty list if string properties are wrong`() {
        val entity = createEntity("urn:ngsi-ld:Beekeeper:1231", listOf("Beekeeper"), mutableListOf(Property(name = "name", value = "Scalpa")))
        val entities: List<String> = neo4jRepository.getEntitiesByTypeAndQuery("Beekeeper", Pair(listOf(), listOf(Triple("name", "=", "ScalpaXYZ"))))
        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if type and integer properties are correct`() {
        val entity = createEntity("urn:ngsi-ld:DeadFishes:019BN", listOf("DeadFishes"), mutableListOf(Property(name = "fishNumber", value = 500)))
        val entities: List<String> = neo4jRepository.getEntitiesByTypeAndQuery("DeadFishes", Pair(listOf(), listOf(Triple("fishNumber", "=", "500"))))
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an empty list if integer properties are wrong`() {
        val entity = createEntity("urn:ngsi-ld:DeadFishes:019BO", listOf("DeadFishes"), mutableListOf(Property(name = "fishNumber", value = 500)))
        val entities: List<String> = neo4jRepository.getEntitiesByTypeAndQuery("DeadFishes", Pair(listOf(), listOf(Triple("fishNumber", "=", "499"))))
        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if type and float properties are correct`() {
        val entity = createEntity("urn:ngsi-ld:DeadFishes:019BP", listOf("DeadFishes"), mutableListOf(Property(name = "fishWeight", value = 120.50)))
        val entities: List<String> = neo4jRepository.getEntitiesByTypeAndQuery("DeadFishes", Pair(listOf(), listOf(Triple("fishWeight", "=", "120.50"))))
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an empty list if float properties are wrong`() {
        val entity = createEntity("urn:ngsi-ld:DeadFishes:019BQ", listOf("DeadFishes"), mutableListOf(Property(name = "fishWeight", value = -120.50)))
        val entities: List<String> = neo4jRepository.getEntitiesByTypeAndQuery("DeadFishes", Pair(listOf(), listOf(Triple("fishWeight", "=", "-120"))))
        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an empty list if given weight equals to entity weight and comparaison parameter is wrong`() {
        val entity = createEntity("urn:ngsi-ld:DeadFishes:019BR", listOf("DeadFishes"), mutableListOf(Property(name = "fishWeight", value = 180.9)))
        val entities: List<String> = neo4jRepository.getEntitiesByTypeAndQuery("DeadFishes", Pair(listOf(), listOf(Triple("fishWeight", ">", "180.9"))))
        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if given weight equals to entity weight and comparaison parameter is correct`() {
        val entity = createEntity("urn:ngsi-ld:DeadFishes:019BS", listOf("DeadFishes"), mutableListOf(Property(name = "fishWeight", value = 255)))
        val entities: List<String> = neo4jRepository.getEntitiesByTypeAndQuery("DeadFishes", Pair(listOf(), listOf(Triple("fishWeight", ">=", "255"))))
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an empty list if given name equals to entity name and comparaison parameter is wrong`() {
        val entity = createEntity("urn:ngsi-ld:Beekeeper:1232", listOf("Beekeeper"), mutableListOf(Property(name = "name", value = "ScalpaXYZ")))
        val entities: List<String> = neo4jRepository.getEntitiesByTypeAndQuery("Beekeeper", Pair(listOf(), listOf(Triple("name", "<>", "ScalpaXYZ"))))
        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if given name not equals to entity name and comparaison parameter is correct`() {
        val entity = createEntity("urn:ngsi-ld:Beekeeper:1233", listOf("Beekeeper"), mutableListOf(Property(name = "name", value = "Scalpa")))
        val entities: List<String> = neo4jRepository.getEntitiesByTypeAndQuery("Beekeeper", Pair(listOf(), listOf(Triple("name", "<>", "ScalpaXYZ"))))
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should add modifiedAt value when creating new entity`() {
        val entity = createEntity("urn:ngsi-ld:Beekeeper:1233", listOf("Beekeeper"), mutableListOf(Property(name = "name", value = "Scalpa")))
        assertNotNull(entityRepository.findById("urn:ngsi-ld:Beekeeper:1233").get().modifiedAt)
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
        val entity = createEntity("urn:ngsi-ld:Beekeeper:1233", listOf("Beekeeper"), mutableListOf(Property(name = "name", value = "Scalpa")))
        val modifiedAt = entityRepository.findById("urn:ngsi-ld:Beekeeper:1233").get().modifiedAt
        createEntity("urn:ngsi-ld:Beekeeper:1233", listOf("Beekeeper"), mutableListOf(Property(name = "name", value = "Demha")))
        val updatedModifiedAt = entityRepository.findById("urn:ngsi-ld:Beekeeper:1233").get().modifiedAt
        assertThat(updatedModifiedAt).isAfter(modifiedAt)
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should find a sensor by vendor id and measured property`() {
        val vendorId = "urn:something:9876"
        val entity = createEntity("urn:ngsi-ld:Sensor:1233", listOf("Sensor"), mutableListOf(Property(name = EGM_VENDOR_ID, value = vendorId)))
        val property = createProperty("https://ontology.eglobalmark.com/apic#outgoing")
        val relationship = createRelationship(property, EGM_OBSERVED_BY, entity.id)
        val persistedEntity = neo4jRepository.getObservingSensorEntity(vendorId, EGM_VENDOR_ID, "outgoing")
        assertNotNull(persistedEntity)
        propertyRepository.delete(property)
        relationshipRepository.delete(relationship)
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should not find a sensor by vendor id if measured property does not exist`() {
        val vendorId = "urn:something:9876"
        val entity = createEntity("urn:ngsi-ld:Sensor:1233", listOf("Sensor"), mutableListOf(Property(name = EGM_VENDOR_ID, value = vendorId)))
        val property = createProperty("https://ontology.eglobalmark.com/apic#outgoing")
        val relationship = createRelationship(property, EGM_OBSERVED_BY, entity.id)
        val persistedEntity = neo4jRepository.getObservingSensorEntity(vendorId, EGM_VENDOR_ID, "incoming")
        assertNull(persistedEntity)
        propertyRepository.delete(property)
        relationshipRepository.delete(relationship)
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should not find a sensor if measured property exists but not the vendor id`() {
        val vendorId = "urn:something:9876"
        val entity = createEntity("urn:ngsi-ld:Sensor:1233", listOf("Sensor"), mutableListOf(Property(name = EGM_VENDOR_ID, value = vendorId)))
        val property = createProperty("https://ontology.eglobalmark.com/apic#outgoing")
        val relationship = createRelationship(property, EGM_OBSERVED_BY, entity.id)
        val persistedEntity = neo4jRepository.getObservingSensorEntity("urn:ngsi-ld:Sensor:Unknown", EGM_VENDOR_ID, "outgoing")
        assertNull(persistedEntity)
        propertyRepository.delete(property)
        relationshipRepository.delete(relationship)
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should find a sensor by NGSI-LD id`() {
        val entity = createEntity("urn:ngsi-ld:Sensor:1233", listOf("Sensor"), mutableListOf())
        val persistedEntity = neo4jRepository.getObservingSensorEntity("urn:ngsi-ld:Sensor:1233", EGM_VENDOR_ID, "anything")
        assertNotNull(persistedEntity)
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should not find a sensor if neither NGSI-LD or vendor id matches`() {
        val entity = createEntity("urn:ngsi-ld:Sensor:1233", listOf("Sensor"), mutableListOf(Property(name = EGM_VENDOR_ID, value = "urn:something:9876")))
        val persistedEntity = neo4jRepository.getObservingSensorEntity("urn:ngsi-ld:Sensor:Unknown", EGM_VENDOR_ID, "unknownMeasure")
        assertNull(persistedEntity)
        neo4jRepository.deleteEntity(entity.id)
    }

    fun createEntity(id: String, type: List<String>, properties: MutableList<Property>): Entity {
        val entity = Entity(id = id, type = type, properties = properties)
        entityRepository.save(entity)
        return entity
    }

    fun createProperty(name: String): Property {
        val property = Property(name = name, value = 1.0)
        return propertyRepository.save(property)
    }

    fun createRelationship(subject: Attribute, relationshipType: String, objectId: String): Relationship {
        val relationship = Relationship(type = listOf(relationshipType))
        val persistedRelationship = relationshipRepository.save(relationship)
        subject.relationships.add(persistedRelationship)
        attributeRepository.save(subject)
        neo4jRepository.createRelationshipToEntity(persistedRelationship.id,
            relationshipType.toRelationshipTypeName(), objectId)

        return persistedRelationship
    }
}
