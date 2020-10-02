package com.egm.stellio.entity.repository

import com.egm.stellio.entity.config.TestContainersConfiguration
import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.Property
import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.ActiveProfiles
import java.net.URI

@SpringBootTest
@ActiveProfiles("test")
@Import(TestContainersConfiguration::class)
class EntityRepositoryTests {

    @Autowired
    private lateinit var entityRepository: EntityRepository

    @Autowired
    private lateinit var neo4jRepository: Neo4jRepository

    @Test
    fun `it should add modifiedAt value when creating new entity`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        assertNotNull(entityRepository.getEntityCoreById("urn:ngsi-ld:Beekeeper:1233")!!.modifiedAt)
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should retrieve an entity from a known property id`() {
        val property = Property(name = "name", value = "Scalpa")
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(), listOf("Beekeeper"), mutableListOf(property)
        )
        val loadedProperty = entityRepository.getEntitySpecificProperty(entity.id.toString(), property.id.toString())
        assertEquals(1, loadedProperty.size)
        assertEquals("name", (loadedProperty[0]["property"] as Property).name)
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should not retrieve an entity from an unknown property id`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val loadedProperty = entityRepository.getEntitySpecificProperty(entity.id.toString(), "unknown-property-id")
        assertTrue(loadedProperty.isEmpty())
        neo4jRepository.deleteEntity(entity.id)
    }

    fun createEntity(id: URI, type: List<String>, properties: MutableList<Property>): Entity {
        val entity = Entity(id = id, type = type, properties = properties)
        return entityRepository.save(entity)
    }
}
