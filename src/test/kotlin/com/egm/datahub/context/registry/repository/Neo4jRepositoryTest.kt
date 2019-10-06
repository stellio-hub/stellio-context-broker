package com.egm.datahub.context.registry.repository

import com.egm.datahub.context.registry.IntegrationTestsBase
import com.egm.datahub.context.registry.service.NgsiLdParserService
import com.egm.datahub.context.registry.web.EntityCreationException
import com.google.gson.GsonBuilder
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.neo4j.ogm.response.model.NodeModel
import org.neo4j.ogm.response.model.RelationshipModel
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class Neo4jRepositoryTest : IntegrationTestsBase() {

    @Autowired
    private lateinit var neo4jRepository: Neo4jRepository

    @Autowired
    private lateinit var ngsiLdParserService: NgsiLdParserService

    private val logger = LoggerFactory.getLogger(Neo4jRepositoryTest::class.java)
    private val gson = GsonBuilder().setPrettyPrinting().create()

    @BeforeAll
    fun initializeDbFixtures() {
        logger.info("Initializing fixtures for test")
        cleanDb()
        addURIindex()
        addNamespaces()
        insertFixtures()
    }

    @AfterAll
    fun removeFixtures() {
        logger.info("Removing fixtures for test")
        cleanDb()
    }

    @Test
    fun `query ngsild parking entity by URI`() {

        val result = neo4jRepository.getNodeByURI("urn:example:OffStreetParking:Downtown1")
        val ngsild = ngsiLdParserService.queryResultToNgsiLd(result)
        println(gson.toJson(ngsild))
        val nestedProperty = ngsild.get("availableSpotNumber") as Map<String, Any>
        assertNotNull(nestedProperty.get("type"))
        assertNotNull(nestedProperty.get("value"))
        val nestedRelationshipInProperty = nestedProperty.get("providedBy") as Map<String, Any>
        assertEquals("Relationship", nestedRelationshipInProperty.get("type"))
        assertEquals("urn:example:Camera:C1", nestedRelationshipInProperty.get("object"))
        val location = ngsild.get("location") as Map<String, Any>
        assertNotNull(ngsild.get("location"))
        val value = location.get("value") as Map<String, Any>
        val coords = value.get("coordinates") as Array<Double>
        assertEquals(-8.5, coords[0])
    }

    @Test
    fun `query ngsild vehicle entity by URI`() {

        val result = neo4jRepository.getNodeByURI("urn:example:Vehicle:A4567")
        val ngsild = ngsiLdParserService.queryResultToNgsiLd(result)
        println(gson.toJson(ngsild))
        val relationship = ngsild.get("isParked") as Map<String, Any>
        assertEquals("Relationship", relationship.get("type"))
        assertNotNull(relationship.get("object"))
        assertEquals("urn:example:OffStreetParking:Downtown1", relationship.get("object"))
        val relationshipDepeer = relationship.get("providedBy") as Map<String, Any>
        assertEquals("urn:example:Person:Bob", relationshipDepeer.get("object"))
        assertEquals("Relationship", relationshipDepeer.get("type"))
    }

    @Test
    fun `query entities by label`() {
        val result = neo4jRepository.getEntitiesByLabel("diat__Beekeeper")
        assertEquals(2, result.size)
    }

    @Test
    fun `query entities by query`() {
        val result = neo4jRepository.getEntitiesByQuery("name==ParisBeehive12")
        assertEquals(1, result.size)
    }

    @Test
    fun `query entities by label and query`() {
        val result = neo4jRepository.getEntitiesByLabelAndQuery("name==Scalpa", "diat__Beekeeper")
        assertEquals(1, result.size)
    }

    @Test
    fun `query results parking ngsild`() {

        val entityUri = "urn:example:OffStreetParking:Downtown1"
        // check has one nested properties
        val nestedProperties = neo4jRepository.getNestedPropertiesByURI(entityUri)
        assertEquals(1, nestedProperties.size)
        // check nested property has one relationship
        val nodemodel = nestedProperties.first().get("t") as NodeModel
        val nestedpropUri = nodemodel.property("uri").toString()
        val relationships = neo4jRepository.getRelationshipByURI(nestedpropUri)
        assertEquals(1, relationships.size)
        val reltype = relationships.get(0).get("rel")

        val result: MutableList<Map<String, Any>> = this.neo4jRepository.getEntitiesByLabel("example__OffStreetParking")
        assertEquals(result.size, 1)
    }

    @Test
    fun `query results vehicle ngsild`() {

        val entityUri = "urn:example:Vehicle:A4567"
        // check No nested properties
        val nestedProperties = neo4jRepository.getNestedPropertiesByURI(entityUri)
        assertEquals(0, nestedProperties.size)
        // check (Vehicle)--[isParked]--(Downtown)
        val relationships = neo4jRepository.getRelationshipByURI(entityUri)
        assertEquals(1, relationships.size)
        // check nested relationship has been materialized in a node and the uri is equal to telationship uri
        val nodemodel = relationships.first().get("r") as RelationshipModel
        val matRelUri = nodemodel.propertyList.filter { it.key == "uri" }.map { it.value }.get(0)
        val materializedRel = neo4jRepository.getNodeByURI(matRelUri.toString())
        assertNotNull(materializedRel)
        val matrel = materializedRel.get("n") as NodeModel
        assertTrue(matrel.property("uri").toString().startsWith("urn:example:isParked:"))
        // check nested relationship has a nested relationship isProvidedBy
        val relationshipsDepeer = neo4jRepository.getRelationshipByURI(matRelUri.toString())
        assertEquals(1, relationshipsDepeer.size)
        val result: MutableList<Map<String, Any>> = this.neo4jRepository.getEntitiesByLabel("example__Vehicle")
        assertEquals(result.size, 1)
    }

    @Test
    fun `update on cypherOnRdf by payload, URI and attribute`() {
        val file = ClassPathResource("/ngsild/sensor_update.json")
        val content = file.inputStream.readBytes().toString(Charsets.UTF_8)
        val uri: String = "urn:diat:Sensor:0022CCC"
        val entityBefore = neo4jRepository.getNodeByURI(uri).get("n") as NodeModel
        val updateQuery = ngsiLdParserService.ngsiLdToUpdateQuery(content, uri, "name")
        this.neo4jRepository.updateEntity(updateQuery, uri)
        val entityAfter = neo4jRepository.getNodeByURI(uri).get("n") as NodeModel
        assertNotEquals(entityBefore.propertyList.find { it.key == "name" }, entityAfter.propertyList.find { it.key == "name" })
        assertNotNull(entityAfter.propertyList.find { it.key == "modifiedAt" })
    }

    fun addNamespaces() {
        val addNamespaces = "CREATE (:NamespacePrefixDefinition {\n" +
                "  `https://diatomic.eglobalmark.com/ontology#`: 'diat',\n" +
                "  `http://xmlns.com/foaf/0.1/`: 'foaf',\n" +
                "  `https://uri.etsi.org/ngsi-ld/v1/ontology#`: 'ngsild'})"
        val resultSummary = driver().session().run(addNamespaces, emptyMap()).consume()
        logger.debug("Nodes created ${resultSummary.counters().nodesCreated()}")
    }

    fun addURIindex() {
        val resultSummary = driver().session().run("CREATE INDEX ON :Resource(uri)").consume()
        logger.debug("Indexes added : ${resultSummary.counters().indexesAdded()}")
    }

    fun cleanDb() {
        val resultSummary = driver().session().run("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r").consume()
        logger.debug("Node deleted ${resultSummary.counters().nodesDeleted()}")
    }

    fun insertFixtures() {

        listOf(
            ClassPathResource("/ngsild/beekeeper.json"),
            ClassPathResource("/ngsild/beekeeper_notconnected.json"),
            ClassPathResource("/ngsild/beehive.json"),
            ClassPathResource("/ngsild/door.json"),
            ClassPathResource("/ngsild/observation_door.json"),
            ClassPathResource("/ngsild/observation_sensor.json"),
            ClassPathResource("/ngsild/sensor.json"),
            ClassPathResource("/ngsild/smartdoor.json"),
            ClassPathResource("/ngsild/parking_ngsild.json"),
            ClassPathResource("/ngsild/vehicle_ngsild.json")
        ).forEach {
            val content = it.inputStream.readBytes().toString(Charsets.UTF_8)
            try {
                val parsedContent = ngsiLdParserService.parseEntity(content)
                neo4jRepository.createEntity(parsedContent.first, Pair(parsedContent.second, parsedContent.third))
            } catch (e: EntityCreationException) {
                logger.warn("Entity already exists $it")
            } catch (e: Exception) {
                fail("Unable to bootstrap fixtures", e)
            }
        }
    }
}
