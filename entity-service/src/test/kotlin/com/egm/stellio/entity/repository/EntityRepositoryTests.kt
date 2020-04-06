package com.egm.stellio.entity.repository

import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.Property
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest
@ActiveProfiles("test")
class EntityRepositoryTests {

    @Autowired
    private lateinit var entityRepository: EntityRepository

    @Autowired
    private lateinit var neo4jRepository: Neo4jRepository

    @Test
    fun `it should add modifiedAt value when creating new entity`() {
        val entity = createEntity("urn:ngsi-ld:Beekeeper:1233", listOf("Beekeeper"), mutableListOf(Property(name = "name", value = "Scalpa")))
        assertNotNull(entityRepository.findById("urn:ngsi-ld:Beekeeper:1233").get().modifiedAt)
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should retrieve an entity from a known property id`() {
        val property = Property(name = "name", value = "Scalpa")
        val entity = createEntity("urn:ngsi-ld:Beekeeper:1233", listOf("Beekeeper"), mutableListOf(property))
        val loadedProperty = entityRepository.getEntitySpecificProperty(entity.id, property.id)
        assertEquals(1, loadedProperty.size)
        assertEquals("name", (loadedProperty[0]["property"] as Property).name)
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should not retrieve an entity from an unknown property id`() {
        val entity = createEntity("urn:ngsi-ld:Beekeeper:1233", listOf("Beekeeper"), mutableListOf(Property(name = "name", value = "Scalpa")))
        val loadedProperty = entityRepository.getEntitySpecificProperty(entity.id, "unknown-property-id")
        assertTrue(loadedProperty.isEmpty())
        neo4jRepository.deleteEntity(entity.id)
    }

    fun createEntity(id: String, type: List<String>, properties: MutableList<Property>): Entity {
        val entity = Entity(id = id, type = type, properties = properties)
        return entityRepository.save(entity)
    }
}
