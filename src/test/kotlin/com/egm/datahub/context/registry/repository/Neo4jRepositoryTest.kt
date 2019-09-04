package com.egm.datahub.context.registry.repository

import com.egm.datahub.context.registry.config.properties.Neo4jProperties
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import junit.framework.Assert.assertEquals
import junit.framework.Assert.assertTrue
import org.junit.Assert.assertNotEquals
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
import org.testcontainers.containers.Neo4jContainer
import org.testcontainers.containers.Network
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.MountableFile




class KtNeo4jContainer(val imageName: String) : Neo4jContainer<KtNeo4jContainer>(imageName)

@SpringBootTest
@Testcontainers
@EmbeddedKafka(topics = ["entities"])
class Neo4jRepositoryTest() {

    @MockkBean
    private lateinit var neo4jProperties: Neo4jProperties

    private lateinit var sessionFactory: SessionFactory

    private val logger = LoggerFactory.getLogger(Neo4jRepositoryTest::class.java)

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
        val databaseServer: KtNeo4jContainer = KtNeo4jContainer("$IMAGE_NAME:$TAG_NAME")
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
            //.withClasspathResourceMapping("/db", "/data",  BindMode.READ_WRITE)

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
    fun initializeDbFixtures(){
        insertFixtures()
    }


    @Test
    fun `query on cypherOnRdf by label`() {
        every { neo4jProperties.nsmntx } returns databaseServer.httpUrl+"/rdf/cypheronrdf"
        every { neo4jProperties.username } returns "neo4j"
        every { neo4jProperties.password } returns "neo4j"

        val result : List<Map<String, Any>> = this.neo4jRepository.getEntitiesByLabel("diat__Beekeeper")
        assertEquals(result.size, 2)
    }

    @Test
    fun `query on cypherOnRdf by query`() {
        every { neo4jProperties.nsmntx } returns databaseServer.httpUrl+"/rdf/cypheronrdf"
        every { neo4jProperties.username } returns "neo4j"
        every { neo4jProperties.password } returns "neo4j"

        val result : List<Map<String, Any>> = this.neo4jRepository.getEntitiesByQuery("foaf__name==ParisBeehive12")
        assertEquals(result.size, 1)
    }

    @Test
    fun `query on cypherOnRdf by label and query`() {
        every { neo4jProperties.nsmntx } returns databaseServer.httpUrl+"/rdf/cypheronrdf"
        every { neo4jProperties.username } returns "neo4j"
        every { neo4jProperties.password } returns "neo4j"

        val result : List<Map<String, Any>> = this.neo4jRepository.getEntitiesByLabelAndQuery("foaf__name==ParisBeehive12", "diat__BeeHive")
        assertEquals(result.size, 1)
    }

    @Test
    fun `update on cypherOnRdf by payload, URI and attribute`() {
        every { neo4jProperties.nsmntx } returns databaseServer.httpUrl+"/rdf/cypheronrdf"
        every { neo4jProperties.username } returns "neo4j"
        every { neo4jProperties.password } returns "neo4j"

        val file = ClassPathResource("/data/sensor_update.jsonld")
        val content = file.inputStream.readBytes().toString(Charsets.UTF_8)
        val entityBefore = this.neo4jRepository.getNodesByURI("urn:ngsi-ld:Sensor:0022CCC")
        this.neo4jRepository.updateEntity(content,"urn:ngsi-ld:Sensor:0022CCC", "name")
        val entityAfter = this.neo4jRepository.getNodesByURI("urn:ngsi-ld:Sensor:0022CCC")
        assertNotEquals(entityBefore, entityAfter)
    }

    fun addNamespaces(){
        val addNamespaces = "CREATE (:NamespacePrefixDefinition {\n" +
                "  `https://diatomic.eglobalmark.com/ontology#`: 'diat',\n" +
                "  `http://xmlns.com/foaf/0.1/`: 'foaf',\n" +
                "  `https://uri.etsi.org/ngsi-ld/v1/ontology#`: 'ngsild'})"
        GraphDatabase.driver(databaseServer.boltUrl, AuthTokens.none()).use({ driver ->
            driver.session().use({ session ->
                val execResult : StatementResult = session.run(addNamespaces, emptyMap<String, Any>())
                execResult.list().map {
                    println(it)
                    println("added namespaces *************")
                }


            })
        })
    }
    fun addURIindex(){
        GraphDatabase.driver(databaseServer.boltUrl, AuthTokens.none()).use({ driver ->
            driver.session().use({ session ->
                val execResult : StatementResult = session.run("CREATE INDEX ON :Resource(uri)", emptyMap<String, Any>())
                execResult.list().map {
                    println(it)
                    println("added index *************")
                }


            })
        })
    }
    fun cleanDb(){
        GraphDatabase.driver(databaseServer.boltUrl, AuthTokens.none()).use({ driver ->
            driver.session().use({ session ->
                val execResult : StatementResult = session.run("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r", emptyMap<String, Any>())
                execResult.list().map {
                    println(it)
                }


            })
        })
    }
    fun insertFixtures(){

        val listOfFiles = listOf(
            ClassPathResource("/data/beekeeper.jsonld"),
            ClassPathResource("/data/beehive.jsonld"),
            ClassPathResource("/data/beehive_not_connected.jsonld"),
            ClassPathResource("/data/door.jsonld"),
            ClassPathResource("/data/observation_door.jsonld"),
            ClassPathResource("/data/observation_sensor.jsonld"),
            ClassPathResource("/data/sensor.jsonld"),
            ClassPathResource("/data/smartdoor.jsonld")
        )
        for (item in listOfFiles) {
            val content = item.inputStream.readBytes().toString(Charsets.UTF_8)
            try{
                var triplesLoaded : Long= this.neo4jRepository.createEntity(content)
                assertTrue("At least one triple loaded", triplesLoaded > 0)
            } catch(e : Exception){
                logger.error("already existing "+item)
            }


        }
    }

    @Test
    fun `parse ngsild`(){
        val jsonLdFile = ClassPathResource("{\n  \"id\": \"urn:ngsi-ld:OffStreetParking:Downtown1\",\n  \"type\": \"OffStreetParking\",\n  \"name\": {\n    \"type\": \"Property\",\n    \"value\": \"Downtown One\"\n  },\n  \"availableSpotNumber\": {\n    \"type\": \"Property\",\n    \"value\": 121,\n    \"observedAt\": \"2017-07-29T12:05:02Z\",\n    \"reliability\": {\n      \"type\": \"Property\",\n      \"value\": 0.7\n    },\n    \"providedBy\": {\n      \"type\": \"Relationship\",\n      \"object\": \"urn:ngsi-ld:Camera:C1\"\n    }\n  },\n  \"totalSpotNumber\": {\n    \"type\": \"Property\",\n    \"value\": 200\n  },\n  \"location\": {\n    \"type\": \"GeoProperty\",\n    \"value\": {\n      \"type\": \"Point\",\n      \"coordinates\": [-8.5, 41.2]\n    }\n  },\n  \"@context\": [\n    \"file://ngsild/ngsi-ld-core-context.jsonld\"\n  ]\n} ")
        val content = jsonLdFile.inputStream.readBytes().toString(Charsets.UTF_8)
        neo4jRepository.parseNgsiLDandWriteToNeo4j(content)
    }


}