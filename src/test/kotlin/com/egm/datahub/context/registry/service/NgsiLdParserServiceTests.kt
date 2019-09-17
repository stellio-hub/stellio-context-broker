package com.egm.datahub.context.registry.service

import com.egm.datahub.context.registry.util.KtMatches.Companion.ktMatches
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource
import org.springframework.test.context.ActiveProfiles

import org.hamcrest.Matchers.*
import org.hamcrest.MatcherAssert.assertThat

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [ NgsiLdParserService::class ])
@ActiveProfiles("test")
class NgsiLdParserServiceTests {

    @Autowired
    private lateinit var ngsiLdParserService: NgsiLdParserService

    private val logger = LoggerFactory.getLogger(NgsiLdParserServiceTests::class.java)

    @Test
    fun `it should create a simple node with simple properties`() {
        val expectedCreateStatement =
            """
                CREATE (a : diat__Beekeeper {  name: "Scalpa",  uri: "urn:diat:Beekeeper:Pascal"}) return a
            """.trimIndent()
        val beekeeper = ClassPathResource("/ngsild/beekeeper.json")
        val parsingResult = ngsiLdParserService.parseEntity(beekeeper.inputStream.readBytes().toString(Charsets.UTF_8))

        assertThat(parsingResult.first, equalTo("urn:diat:Beekeeper:Pascal"))
        assertThat(parsingResult.second.first.size, equalTo(1))
        assertThat(parsingResult.second.second.size, equalTo(0))
        assertThat(parsingResult.second.first[0], equalTo(expectedCreateStatement))
    }

    @Test
    fun `it should create a node with a relationship`() {
        val expectedMatchStatement =
            """
                MATCH (a : diat__Door {  uri: "urn:diat:Door:0015"}), (b : diat__SmartDoor {  uri: "urn:diat:SmartDoor:0021"}) 
                CREATE (a)-[r:ngsild__connectsTo]->(b) return a,b
            """.trimIndent()
        val door = ClassPathResource("/ngsild/door.json")
        val parsingResult = ngsiLdParserService.parseEntity(door.inputStream.readBytes().toString(Charsets.UTF_8))

        logger.debug("Cypher queries are $parsingResult")
        assertThat("urn:diat:Door:0015", equalTo(parsingResult.first))
        assertThat(parsingResult.second.first.size, equalTo(2))
        assertThat(parsingResult.second.second.size, equalTo(1))
        assertThat(parsingResult.second.second[0], equalToCompressingWhiteSpace(expectedMatchStatement))
    }

    @Test
    fun `it should create a node with an externalized property`() {
        val expectedHasMeasureCreateStatement =
            """
                CREATE \(a : diat__hasMeasure \{ value: "45", unitCode: "C", observedAt: "2019-09-26T21:32:52\+02:00",  
                        uri: "urn:diat:hasMeasure:[a-zA-Z\-0-9]+"}\) return a
            """.trimIndent()
        val expectedObservationCreateStatement =
            """
                CREATE (a : diat__Observation {  uri: "urn:diat:Observation:001112"}) return a
            """.trimIndent()
        val expectedMatchStatement =
            """
                MATCH \(a : diat__Observation \{ uri: "urn:diat:Observation:001112" }\), 
                      \(b : diat__hasMeasure \{ uri: "urn:diat:hasMeasure:[a-zA-Z\-0-9]+" }\) 
                CREATE \(a\)-\[r:ngsild__hasObject]->\(b\) return a,b
             """.trimIndent()
        val observationSensor = ClassPathResource("/ngsild/observation_sensor_prop_only.json")
        val parsingResult = ngsiLdParserService.parseEntity(observationSensor.inputStream.readBytes().toString(Charsets.UTF_8))

        assertThat("urn:diat:Observation:001112", equalTo(parsingResult.first))
        assertThat(parsingResult.second.first.size, equalTo(2))
        assertThat(parsingResult.second.second.size, equalTo(1))
        assertThat(parsingResult.second.first, hasItem(ktMatches(expectedHasMeasureCreateStatement)))
        assertThat(parsingResult.second.first, hasItem(expectedObservationCreateStatement))
        assertThat(parsingResult.second.second[0], ktMatches(expectedMatchStatement))
    }
}