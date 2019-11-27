package com.egm.datahub.context.registry.repository

import com.egm.datahub.context.registry.Neo4jEmbeddedConfiguration
import com.egm.datahub.context.registry.service.Neo4jService
import com.egm.datahub.context.registry.service.NgsiLdParserService
import com.egm.datahub.context.registry.web.AlreadyExistsException
import com.google.gson.GsonBuilder
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.neo4j.ogm.response.model.NodeModel
import org.neo4j.ogm.session.Session
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.core.io.ClassPathResource

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Import(Neo4jEmbeddedConfiguration::class)
class Neo4jRepositoryTest {

    @Autowired
    private lateinit var neo4jRepository: Neo4jRepository

    @Autowired
    private lateinit var neo4jService: Neo4jService

    @Autowired
    private lateinit var ngsiLdParserService: NgsiLdParserService

    @Autowired
    private lateinit var session: Session

    private val logger = LoggerFactory.getLogger(Neo4jRepositoryTest::class.java)
    private val gson = GsonBuilder().setPrettyPrinting().create()

    @BeforeAll
    fun initializeDbFixtures() {
        logger.info("Initializing fixtures for test")
        cleanDb()
        addURIindex()
        insertFixtures()
    }

    @AfterAll
    fun removeFixtures() {
        logger.info("Removing fixtures for test")
        cleanDb()
    }

    @Test
    fun `cant insert duplicated entity (with same urn)`() {
        val resource = ClassPathResource("/ngsild/beekeeper.json")
        val content = resource.inputStream.readBytes().toString(Charsets.UTF_8)
        val urn = ngsiLdParserService.extractEntityUrn(content)
        // an entity with same urn is already provided in fixtures
        assertTrue(neo4jRepository.checkExistingUrn(urn))
    }

    @Test
    fun `query ngsild parking entity by URI`() {

        val result = neo4jRepository.getNodeByURI("urn:ngsi-ld:OffStreetParking:Downtown1")
        val ngsild = neo4jService.queryResultToNgsiLd(result["n"] as NodeModel)
        println(gson.toJson(ngsild))
        val nestedProperty = ngsild.get("availableSpotNumber") as Map<String, Any>
        assertNotNull(nestedProperty.get("type"))
        assertNotNull(nestedProperty.get("value"))
        val nestedRelationshipInProperty = nestedProperty.get("providedBy") as Map<String, Any>
        assertEquals("Relationship", nestedRelationshipInProperty.get("type"))
        assertEquals("urn:ngsi-ld:Camera:C1", nestedRelationshipInProperty.get("object"))
        val location = ngsild.get("location") as Map<String, Any>
        assertNotNull(ngsild.get("location"))
        val value = location.get("value") as Map<String, Any>
        val coords = value.get("coordinates") as Array<Double>
        assertEquals(-8.5, coords[0])
    }

    @Test
    fun `query ngsild vehicle entity by URI`() {

        val result = neo4jRepository.getNodeByURI("urn:ngsi-ld:Vehicle:A4567")
        val ngsild = neo4jService.queryResultToNgsiLd(result["n"] as NodeModel)
        println(gson.toJson(ngsild))
        val relationship = ngsild.get("isParked") as Map<String, Any>
        assertEquals("Relationship", relationship.get("type"))
        assertNotNull(relationship.get("object"))
        assertEquals("urn:ngsi-ld:OffStreetParking:Downtown1", relationship.get("object"))
        val relationshipDepeer = relationship.get("providedBy") as Map<String, Any>
        assertEquals("urn:ngsi-ld:Person:Bob", relationshipDepeer.get("object"))
        assertEquals("Relationship", relationshipDepeer.get("type"))
    }

    @Test
    fun `query entities by label`() {
        val result = neo4jRepository.getEntitiesByLabel("diat__Beekeeper")
        assertEquals(2, result.size)
    }

    @Test
    fun `query entities by query`() {
        val result = neo4jRepository.getEntitiesByLabelAndQuery(listOf("name==ParisBeehive12"), null)
        assertEquals(1, result.size)
    }

    @Test
    fun `query entities by label and query`() {
        val result = neo4jRepository.getEntitiesByLabelAndQuery(listOf("name==Scalpa"), "diat__Beekeeper")
        assertEquals(1, result.size)
    }

    @Test
    fun `query results parking ngsild`() {

        val entityUri = "urn:ngsi-ld:OffStreetParking:Downtown1"
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

        val entityUri = "urn:ngsi-ld:Vehicle:A4567"
        // check No nested properties
        val nestedProperties = neo4jRepository.getNestedPropertiesByURI(entityUri)
        assertEquals(0, nestedProperties.size)
        // check (Vehicle)--[isParked]--(Downtown)
        val relationships = neo4jRepository.getRelationshipByURI(entityUri)
        assertEquals(1, relationships.size)
        // check nested relationship has been materialized in a node and the uri is equal to telationship uri
        val matRelUri = relationships.first()["relUri"]
        val materializedRel = neo4jRepository.getNodeByURI(matRelUri.toString())
        assertNotNull(materializedRel)
        val matrel = materializedRel.get("n") as NodeModel
        assertTrue(matrel.property("uri").toString().startsWith("urn:ngsi-ld:isParked:"))
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
        val uri: String = "urn:ngsi-ld:Sensor:0022CCC"
        val entityBefore = neo4jRepository.getNodeByURI(uri).get("n") as NodeModel
        val updateQuery = ngsiLdParserService.ngsiLdToUpdateEntityAttributeQuery(content, uri, "name")
        this.neo4jRepository.updateEntity(updateQuery, uri)
        val entityAfter = neo4jRepository.getNodeByURI(uri).get("n") as NodeModel
        assertNotEquals(entityBefore.propertyList.find { it.key == "name" }, entityAfter.propertyList.find { it.key == "name" })
        assertNotNull(entityAfter.propertyList.find { it.key == "modifiedAt" })
    }

    @Test
    fun `it should delete properties and relationships when deleting an entity`() {
        val entity = """
            {
              "id": "urn:ngsi-ld:Vehicle:ToBeDeleted",
              "type": "Vehicle",
              "brandName": {
                "type": "Property",
                "value": "Mercedes"
              },
              "name": "name of vehicle 1",
              "remainingEnergy": {
                "type": "Property",
                "value": 70,
                "observedAt": "2017-07-29T12:05:02Z",
                "reliability": {
                  "type": "Property",
                  "value": 0.9
                },
                "providedBy": {
                  "type": "Relationship",
                  "object": "urn:ngsi-ld:EnergyCounter:EC1"
                }
              },
              "location": {
                "type": "GeoProperty",
                "value": {
                  "type": "Point",
                  "coordinates": [-8.5, 41.2]
                }
              },
              "isParked": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:OffStreetParking:Downtown1",
                "observedAt": "2017-07-29T12:00:04Z",
                "providedBy": {
                  "type": "Relationship",
                  "object": "urn:ngsi-ld:Person:Bob"
                }
              },
              "@context": [
                "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
                "http://example.org/ngsi-ld/commonTerms.jsonld",
                "http://example.org/ngsi-ld/vehicle.jsonld",
                "http://example.org/ngsi-ld/parking.jsonld"
              ]
            }
        """.trimIndent()

        val isParkedEntitiesNb = neo4jRepository.getEntitiesByLabel("example__isParked").size

        val ngsiLdParsedResult = ngsiLdParserService.parseEntity(entity)
        neo4jRepository.createEntity(ngsiLdParsedResult.entityUrn, ngsiLdParsedResult.entityStatements,
            ngsiLdParsedResult.relationshipStatements)

        val deleteResult = neo4jRepository.deleteEntity("urn:ngsi-ld:Vehicle:ToBeDeleted")

        assertEquals(3, deleteResult.first)
        assertEquals(4, deleteResult.second)

        assertTrue(neo4jRepository.getNodeByURI("urn:ngsi-ld:Vehicle:ToBeDeleted").isEmpty())
        assertTrue(neo4jRepository.getEntitiesByLabel("ngsild__remainingEnergy").isEmpty())
        assertEquals(isParkedEntitiesNb, neo4jRepository.getEntitiesByLabel("example__isParked").size)

        assertTrue(neo4jRepository.getNodeByURI("urn:ngsi-ld:EnergyCounter:EC1").isNotEmpty())
        assertTrue(neo4jRepository.getNodeByURI("urn:ngsi-ld:OffStreetParking:Downtown1").isNotEmpty())
        assertTrue(neo4jRepository.getNodeByURI("urn:ngsi-ld:Person:Bob").isNotEmpty())
    }

    fun addURIindex() {
        val resultSummary = session.query("CREATE INDEX ON :Resource(uri)", HashMap<String, Any>())
        logger.debug("Indexes added : ${resultSummary.queryStatistics().indexesAdded}")
    }

    fun cleanDb() {
        val resultSummary = session.query("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r", HashMap<String, Any>())
        logger.debug("Node deleted ${resultSummary.queryStatistics().nodesDeleted}")
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
                val ngsiLdParsedResult = ngsiLdParserService.parseEntity(content)
                neo4jRepository.createEntity(ngsiLdParsedResult.entityUrn, ngsiLdParsedResult.entityStatements,
                    ngsiLdParsedResult.relationshipStatements)
            } catch (e: AlreadyExistsException) {
                logger.warn("Entity already exists $it")
            } catch (e: Exception) {
                fail("Unable to bootstrap fixtures", e)
            }
        }
    }
}
