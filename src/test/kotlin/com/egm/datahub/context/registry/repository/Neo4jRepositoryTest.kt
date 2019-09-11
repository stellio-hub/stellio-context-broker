package com.egm.datahub.context.registry.repository

import com.egm.datahub.context.registry.IntegrationTestsBase
import com.egm.datahub.context.registry.service.NgsiLdParserService
import com.google.gson.GsonBuilder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource

@SpringBootTest
class Neo4jRepositoryTest : IntegrationTestsBase() {

    @Autowired
    private lateinit var neo4jRepository: Neo4jRepository

    @Autowired
    private lateinit var ngsiLdParserService: NgsiLdParserService

    private val logger = LoggerFactory.getLogger(Neo4jRepositoryTest::class.java)
    private val gson = GsonBuilder().setPrettyPrinting().create()

    @BeforeAll
    fun initializeDbFixtures() {
        cleanDb()
        addURIindex()
        addNamespaces()
        insertFixtures()
    }

    @Test
    fun `query on cypherOnRdf by label`() {
        val result = this.neo4jRepository.getEntitiesByLabel("diat__Beekeeper")
        assertEquals(2, result.size)
    }

    @Test
    fun `query on cypherOnRdf by query`() {
        val result = this.neo4jRepository.getEntitiesByQuery("name==ParisBeehive12")
        assertEquals(1, result.size)
    }

    @Test
    fun `query on cypherOnRdf by label and query`() {
        val result = this.neo4jRepository.getEntitiesByLabelAndQuery("name==Scalpa", "diat__Beekeeper")
        assertEquals(1, result.size)
    }

    // @Test
    fun `update on cypherOnRdf by payload, URI and attribute`() {
        val file = ClassPathResource("/ngsild/sensor_update.jsonld")
        val content = file.inputStream.readBytes().toString(Charsets.UTF_8)
        val entityBefore = this.neo4jRepository.getNodesByURI("urn:diat:Sensor:0022CCC")
        this.neo4jRepository.updateEntity(content, "urn:diat:Sensor:0022CCC", "name")
        val entityAfter = this.neo4jRepository.getNodesByURI("urn:diat:Sensor:0022CCC")
        assertFalse(gson.toJson(entityBefore) == gson.toJson(entityAfter))
    }

    fun addNamespaces() {
        val addNamespaces = "CREATE (:NamespacePrefixDefinition {\n" +
                "  `https://diatomic.eglobalmark.com/ontology#`: 'diat',\n" +
                "  `http://xmlns.com/foaf/0.1/`: 'foaf',\n" +
                "  `https://uri.etsi.org/ngsi-ld/v1/ontology#`: 'ngsild'})"
        val resultSummary = driver().session().run(addNamespaces, emptyMap()).consume()
        logger.debug("Got result ${resultSummary.counters()}")
    }

    fun addURIindex() {
        val resultSummary = driver().session().run("CREATE INDEX ON :Resource(uri)").consume()
        logger.debug("Got result ${resultSummary.counters()}")
    }

    fun cleanDb() {
        val resultSummary = driver().session().run("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r").consume()
        logger.debug("Got result ${resultSummary.counters()}")
    }

    fun insertFixtures() {

        val listOfFiles = listOf(
            ClassPathResource("/ngsild/beekeeper.json"),
            ClassPathResource("/ngsild/beekeeper_notconnected.json"),
            ClassPathResource("/ngsild/beehive.json"),
            ClassPathResource("/ngsild/door.json"),
            ClassPathResource("/ngsild/observation_door.json"),
            ClassPathResource("/ngsild/observation_sensor.json"),
            ClassPathResource("/ngsild/sensor.json"),
            ClassPathResource("/ngsild/smartdoor.json")
        )
        for (item in listOfFiles) {
            val content = item.inputStream.readBytes().toString(Charsets.UTF_8)
            try {
                val parsedContent = ngsiLdParserService.parseEntity(content)
                neo4jRepository.createEntity("", parsedContent.second)
            } catch (e: Exception) {
                logger.error("already existing $item")
            }
        }
    }
}