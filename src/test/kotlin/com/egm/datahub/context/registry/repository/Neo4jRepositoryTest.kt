package com.egm.datahub.context.registry.repository

import com.egm.datahub.context.registry.IntegrationTestsBase
import com.egm.datahub.context.registry.service.NgsiLdParserService
import com.egm.datahub.context.registry.web.EntityCreationException
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.io.ClassPathResource

class Neo4jRepositoryTest : IntegrationTestsBase() {

    @Autowired
    private lateinit var neo4jRepository: Neo4jRepository

    @Autowired
    private lateinit var ngsiLdParserService: NgsiLdParserService

    private val logger = LoggerFactory.getLogger(Neo4jRepositoryTest::class.java)

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
    fun `query on cypherOnRdf by label`() {
        val result = neo4jRepository.getEntitiesByLabel("diat__Beekeeper")
        assertEquals(2, result.size)
    }

    @Test
    fun `query on cypherOnRdf by query`() {
        val result = neo4jRepository.getEntitiesByQuery("name==ParisBeehive12")
        assertEquals(1, result.size)
    }

    @Test
    fun `query on cypherOnRdf by label and query`() {
        val result = neo4jRepository.getEntitiesByLabelAndQuery("name==Scalpa", "diat__Beekeeper")
        assertEquals(1, result.size)
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
            ClassPathResource("/ngsild/smartdoor.json")
        ).forEach {
            val content = it.inputStream.readBytes().toString(Charsets.UTF_8)
            try {
                val parsedContent = ngsiLdParserService.parseEntity(content)
                neo4jRepository.createOrUpdateEntity("", parsedContent.second)
            } catch (e: EntityCreationException) {
                logger.warn("Entity already exists $it")
            } catch (e: Exception) {
                fail("Unable to bootstrap fixtures", e)
            }
        }
    }
}
