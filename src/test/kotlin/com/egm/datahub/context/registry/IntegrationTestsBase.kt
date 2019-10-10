package com.egm.datahub.context.registry

import com.egm.datahub.context.registry.config.properties.Neo4jProperties
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.Config
import org.neo4j.driver.v1.Driver
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.kernel.configuration.BoltConnector
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles
import java.io.File

@EmbeddedKafka(topics = ["entities"])
@ActiveProfiles("test")
open class IntegrationTestsBase {

    @Autowired
    private lateinit var neo4jProperties: Neo4jProperties

    private lateinit var graphDb: GraphDatabaseService

    private val logger = LoggerFactory.getLogger(IntegrationTestsBase::class.java)

    init {
        System.setProperty(EmbeddedKafkaBroker.BROKER_LIST_PROPERTY, "spring.kafka.bootstrap-servers")
    }

    @BeforeAll
    private fun startNeo4jEmbed() {
        logger.info("Starting Neo4j")

        val bolt = BoltConnector()

        graphDb = GraphDatabaseFactory()
            .newEmbeddedDatabaseBuilder(File("tmp/db.data"))
            .setConfig(bolt.type, "BOLT")
            .setConfig(bolt.enabled, "true")
            .setConfig(bolt.listen_address, "localhost:7687")
            .setConfig(bolt.encryption_level, "DISABLED")
            .newGraphDatabase()
    }

    @AfterAll
    private fun shutdownNeo4jEmbed() {
        logger.info("Shutting down Neo4j")

        graphDb.shutdown()
    }

    fun driver(): Driver {
        val config = Config.build()
            .withoutEncryption()
            .toConfig()

        return GraphDatabase.driver(neo4jProperties.uri,
            AuthTokens.basic(neo4jProperties.username, neo4jProperties.password),
            config)
    }
}
