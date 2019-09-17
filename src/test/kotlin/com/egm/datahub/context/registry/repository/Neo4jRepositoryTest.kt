package com.egm.datahub.context.registry.repository

import com.egm.datahub.context.registry.config.properties.Neo4jProperties
import com.google.gson.GsonBuilder
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import junit.framework.Assert.assertEquals
import org.junit.Assert.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.driver.v1.StatementResult
import org.neo4j.ogm.config.Configuration
import org.neo4j.ogm.session.SessionFactory
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.core.io.ClassPathResource
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.testcontainers.containers.Neo4jContainer
import org.testcontainers.containers.Network
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.MountableFile

class KtNeo4jContainer(imageName: String) : Neo4jContainer<KtNeo4jContainer>(imageName)

@SpringBootTest
@Testcontainers
@DirtiesContext
@EmbeddedKafka(topics = ["entities"])
class Neo4jRepositoryTest() {

    @MockkBean
    private lateinit var neo4jProperties: Neo4jProperties

    private lateinit var sessionFactory: SessionFactory

    private val logger = LoggerFactory.getLogger(Neo4jRepositoryTest::class.java)
    private val gson = GsonBuilder().setPrettyPrinting().create()

    @Autowired
    private lateinit var neo4jRepository: Neo4jRepository

    @TestConfiguration // <2>
    internal class Config {

        @Bean // <3>
        fun configuration(): org.neo4j.ogm.config.Configuration {
            return Configuration.Builder()
                .uri(databaseServer.boltUrl)
                .build()
        }
    }

    companion object {
        const val IMAGE_NAME = "neo4j"
        const val TAG_NAME = "3.5.8"

        var network = Network.newNetwork()

        @Container
        @JvmStatic
        val databaseServer: KtNeo4jContainer = KtNeo4jContainer(
            "$IMAGE_NAME:$TAG_NAME"
        )
            .withPlugins(MountableFile.forHostPath("data/neo4j/plugins"))
            .withNeo4jConfig("dbms.unmanaged_extension_classes", "semantics.extension=/rdf")
            .withNeo4jConfig("dbms.security.procedures.whitelist", "semantics.*,apoc.*")
            .withNeo4jConfig("dbms.security.procedures.unrestricted", "semantics.*,apoc.*")
            .withNeo4jConfig("apoc.export.file.enabled", "true")
            .withNeo4jConfig("apoc.import.file.enabled", "true")
            .withNeo4jConfig("apoc.import.file.use_neo4j_config", "true")
            .withExposedPorts(7474, 7687)
            .withEnv("NEO4J_apoc_export_file_enabled", "true")
            .withEnv("NEO4J_apoc_export_file_enabled", "true")
            .withEnv("NEO4J_apoc_import_file_use__neo4j__config", "true")
            .withoutAuthentication()
            .withNetwork(network)
            .withNetworkAliases("dh-local-docker")
            // .withClasspathResourceMapping("/db", "/data",  BindMode.READ_WRITE)
    }

    init {
        System.setProperty(EmbeddedKafkaBroker.BROKER_LIST_PROPERTY, "spring.kafka.bootstrap-servers")
        databaseServer.start()
        try {
            val ogmConfiguration = Configuration.Builder()
                .uri(databaseServer.boltUrl)
                .build()
            sessionFactory = SessionFactory(
                ogmConfiguration,
                "com.egm.datahub.context.registry.domain"
            )
        } catch (e: Exception) {
            println("can't bootstrap ogm session")
        }
        cleanDb()
        addURIindex()
        addNamespaces()
    }

    @BeforeAll
    fun initializeDbFixtures() {
        insertFixtures()
    }

    // @Test
    fun `query on cypherOnRdf by label`() {
        every { neo4jProperties.username } returns "neo4j"
        every { neo4jProperties.password } returns "neo4j"

        val result: MutableList<Map<String, Any>> = this.neo4jRepository.getEntitiesByLabel("diat__Beekeeper")
        assertEquals(result.size, 2)
    }

    // @Test
    fun `query on cypherOnRdf by query`() {
        every { neo4jProperties.username } returns "neo4j"
        every { neo4jProperties.password } returns "neo4j"

        val result: MutableList<Map<String, Any>> = this.neo4jRepository.getEntitiesByQuery("name==ParisBeehive12")
        assertEquals(result.size, 1)
    }

    // @Test
    fun `query on cypherOnRdf by label and query`() {
        every { neo4jProperties.username } returns "neo4j"
        every { neo4jProperties.password } returns "neo4j"

        val result: MutableList<Map<String, Any>> = this.neo4jRepository.getEntitiesByLabelAndQuery("name==Pellizzoni", "diat__Beekeeper")
        assertEquals(result.size, 1)
    }

    // @Test
    fun `update on cypherOnRdf by payload, URI and attribute`() {
        every { neo4jProperties.username } returns "neo4j"
        every { neo4jProperties.password } returns "neo4j"

        val file = ClassPathResource("/ngsild/sensor_update.jsonld")
        val content = file.inputStream.readBytes().toString(Charsets.UTF_8)
        val entityBefore = this.neo4jRepository.getNodesByURI("urn:diat:Sensor:0022CCC")
        this.neo4jRepository.updateEntity(content, "urn:diat:Sensor:0022CCC", "name")
        val entityAfter = this.neo4jRepository.getNodesByURI("urn:diat:Sensor:0022CCC")
        assertNotEquals(gson.toJson(entityBefore), gson.toJson(entityAfter))
    }

    @Test
    fun `insert parking ngsild and query results`() {

        every { neo4jProperties.username } returns "neo4j"
        every { neo4jProperties.password } returns "neo4j"

        val createStatement =
            """
                CREATE (a : diat__Beekeeper {  name: "Scalpa",  uri: "urn:diat:Beekeeper:Pascal"}) return a
            """.trimIndent()
        try {
            // FIXME port to new API
            val entityUri = neo4jRepository.createEntity("urn:diat:Beekeeper:Pascal", Pair(listOf(createStatement), emptyList()))
            assertEquals("urn:diat:Beekeeper:Pascal", entityUri)
        } catch (e: Exception) {
            fail("should not have thrown an exception")
        }

        val result: MutableList<Map<String, Any>> = this.neo4jRepository.getEntitiesByLabel("diat__Beekeeper")
        assertEquals(result.size, 1)
    }

    fun addNamespaces() {
        val addNamespaces = "CREATE (:NamespacePrefixDefinition {\n" +
                "  `https://diatomic.eglobalmark.com/ontology#`: 'diat',\n" +
                "  `http://xmlns.com/foaf/0.1/`: 'foaf',\n" +
                "  `https://uri.etsi.org/ngsi-ld/v1/ontology#`: 'ngsild'})"
        GraphDatabase.driver(databaseServer.boltUrl, AuthTokens.none()).use({ driver ->
            driver.session().use({ session ->
                val execResult: StatementResult = session.run(addNamespaces, emptyMap<String, Any>())
                execResult.list().map {
                    println(it)
                    println("added namespaces *************")
                }
            })
        })
    }

    fun addURIindex() {
        GraphDatabase.driver(databaseServer.boltUrl, AuthTokens.none()).use({ driver ->
            driver.session().use({ session ->
                val execResult: StatementResult = session.run("CREATE INDEX ON :Resource(uri)", emptyMap<String, Any>())
                execResult.list().map {
                    println(it)
                    println("added index *************")
                }
            })
        })
    }

    fun cleanDb() {
        GraphDatabase.driver(databaseServer.boltUrl, AuthTokens.none()).use({ driver ->
            driver.session().use({ session ->
                val execResult: StatementResult = session.run("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r", emptyMap<String, Any>())
                execResult.list().map {
                    println(it)
                }
            })
        })
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
                // FIXME port to new API
                val entityUri = neo4jRepository.createEntity(content, Pair(emptyList(), emptyList()))
                assertTrue(entityUri.isNotEmpty())
            } catch (e: Exception) {
                logger.error("already existing " + item)
            }
        }
    }
}