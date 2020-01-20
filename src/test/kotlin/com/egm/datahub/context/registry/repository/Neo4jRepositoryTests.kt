package com.egm.datahub.context.registry.repository

import com.egm.datahub.context.registry.model.Entity
import com.egm.datahub.context.registry.model.Property
import junit.framework.TestCase.*
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.beans.factory.annotation.Autowired

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("test")
class Neo4jRepositoryTests {

    @Autowired
    private lateinit var neo4jRepository: Neo4jRepository

    @Autowired
    private lateinit var entityRepository: EntityRepository

    @Test
    fun `it should return an entity if type and string properties are correct`() {
        createEntity("urn:ngsi-ld:Beekeeper:1230", listOf("Beekeeper"), mutableListOf(Property(name = "name", value = "Scalpa")))
        val entities: List<String> = neo4jRepository.getEntitiesByTypeAndQuery("Beekeeper", Pair(listOf(), listOf(Pair("name", "Scalpa"))))
        assertEquals(1, entities.size)
        neo4jRepository.deleteEntity("urn:ngsi-ld:Beekeeper:1230")
    }

    @Test
    fun `it should return an empty list if string properties are wrong`() {
        createEntity("urn:ngsi-ld:Beekeeper:1231", listOf("Beekeeper"), mutableListOf(Property(name = "name", value = "Scalpa")))
        val entities: List<String> = neo4jRepository.getEntitiesByTypeAndQuery("Beekeeper", Pair(listOf(), listOf(Pair("name", "ScalpaXYZ"))))
        assertEquals(0, entities.size)
        neo4jRepository.deleteEntity("urn:ngsi-ld:Beekeeper:1231")
    }

    @Test
    fun `it should return an entity if type and integer properties are correct`() {
        createEntity("urn:ngsi-ld:DeadFishes:019BN", listOf("DeadFishes"), mutableListOf(Property(name = "fishNumber", value = 500)))
        val entities: List<String> = neo4jRepository.getEntitiesByTypeAndQuery("DeadFishes", Pair(listOf(), listOf(Pair("fishNumber", "500"))))
        assertEquals(1, entities.size)
        neo4jRepository.deleteEntity("urn:ngsi-ld:DeadFishes:019BN")
    }

    @Test
    fun `it should return an empty list if integer properties are wrong`() {
        createEntity("urn:ngsi-ld:DeadFishes:019BO", listOf("DeadFishes"), mutableListOf(Property(name = "fishNumber", value = 500)))
        val entities: List<String> = neo4jRepository.getEntitiesByTypeAndQuery("DeadFishes", Pair(listOf(), listOf(Pair("fishNumber", "499"))))
        assertEquals(0, entities.size)
        neo4jRepository.deleteEntity("urn:ngsi-ld:DeadFishes:019BO")
    }

    @Test
    fun `it should return an entity if type and float properties are correct`() {
        createEntity("urn:ngsi-ld:DeadFishes:019BP", listOf("DeadFishes"), mutableListOf(Property(name = "fishWeight", value = 120.50)))
        val entities: List<String> = neo4jRepository.getEntitiesByTypeAndQuery("DeadFishes", Pair(listOf(), listOf(Pair("fishWeight", "120.50"))))
        assertEquals(1, entities.size)
        neo4jRepository.deleteEntity("urn:ngsi-ld:DeadFishes:019BP")
    }

    @Test
    fun `it should return an empty list if float properties are wrong`() {
        createEntity("urn:ngsi-ld:DeadFishes:019BQ", listOf("DeadFishes"), mutableListOf(Property(name = "fishWeight", value = -120.50)))
        val entities: List<String> = neo4jRepository.getEntitiesByTypeAndQuery("DeadFishes", Pair(listOf(), listOf(Pair("fishWeight", "-120"))))
        assertEquals(0, entities.size)
        neo4jRepository.deleteEntity("urn:ngsi-ld:DeadFishes:019BQ")
    }

    fun createEntity(id: String, type: List<String>, properties: MutableList<Property>) {
        val entity = Entity(id = id, type = type, properties = properties)
        entityRepository.save(entity)
    }
}
