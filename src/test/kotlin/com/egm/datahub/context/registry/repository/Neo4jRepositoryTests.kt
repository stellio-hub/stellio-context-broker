package com.egm.datahub.context.registry.repository

import com.egm.datahub.context.registry.model.Entity
import com.egm.datahub.context.registry.model.Property
import com.egm.datahub.context.registry.model.Relationship
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

    fun createEntity(id: String, type: List<String>, properties: MutableList<Property>): Entity {
        val entity = Entity(id = id, type = type, properties = properties)
        entityRepository.save(entity)
        return entity
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
}