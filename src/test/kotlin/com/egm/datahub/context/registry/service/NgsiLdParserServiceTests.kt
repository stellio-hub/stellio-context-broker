package com.egm.datahub.context.registry.service

import com.egm.datahub.context.registry.util.KtMatches.Companion.ktMatches
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.equalTo
import org.hamcrest.Matchers.hasItem
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource
import org.springframework.test.context.ActiveProfiles

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
                MERGE \(a : diat__Beekeeper \{ uri: "urn:diat:Beekeeper:Pascal" }\)
                ON CREATE SET a = \{ name: "Scalpa", uri: "urn:diat:Beekeeper:Pascal", createdAt: "[0-9\.]+", modifiedAt: "[0-9\.]+" }
                ON MATCH SET a \+= \{ name: "Scalpa", uri: "urn:diat:Beekeeper:Pascal", createdAt: "[0-9\.]+", modifiedAt: "[0-9\.]+" }
                return a
            """.trimIndent()
        val beekeeper = ClassPathResource("/ngsild/beekeeper.json")
        val parsingResult = ngsiLdParserService.parseEntity(beekeeper.inputStream.readBytes().toString(Charsets.UTF_8))

        assertThat(parsingResult.entityUrn, equalTo("urn:diat:Beekeeper:Pascal"))
        assertThat(parsingResult.entityType, equalTo("Beekeeper"))
        assertThat(parsingResult.entityStatements.size, equalTo(1))
        assertThat(parsingResult.relationshipStatements.size, equalTo(0))
        assertThat(parsingResult.entityStatements[0], ktMatches(expectedCreateStatement))
    }

    @Test
    fun `it should create a node with a geo property`() {
        val expectedCreateStatement =
            """
                MERGE \(a : diat__BeeHive \{ uri: "urn:diat:BeeHive:TESTC"}\)
                ON CREATE SET a = \{ name: "ParisBeehive12", location: "point\(\{ x: 13.3986 , y: 52.5547, crs: \\u0027WGS-84\\u0027 }\)",  
                    uri: "urn:diat:BeeHive:TESTC", createdAt: "[0-9\.]+", modifiedAt: "[0-9\.]+"}
                ON MATCH  SET a \+= \{ name: "ParisBeehive12", location: "point\(\{ x: 13.3986 , y: 52.5547, crs: \\u0027WGS-84\\u0027 }\)", 
                    uri: "urn:diat:BeeHive:TESTC", createdAt: "[0-9\.]+", modifiedAt: "[0-9\.]+"}
                return a
            """.trimIndent()
        val beekeeper = ClassPathResource("/ngsild/beehive_with_geoproperty.json")
        val parsingResult = ngsiLdParserService.parseEntity(beekeeper.inputStream.readBytes().toString(Charsets.UTF_8))

        assertThat(parsingResult.entityUrn, equalTo("urn:diat:BeeHive:TESTC"))
        assertThat(parsingResult.entityStatements.size, equalTo(1))
        assertThat(parsingResult.relationshipStatements.size, equalTo(0))
        assertThat(parsingResult.entityStatements[0], ktMatches(expectedCreateStatement))
    }

    @Test
    fun `it should create a node with a relationship`() {
        val expectedMatchStatement =
            """
                MATCH \(a : diat__Door \{ uri: "urn:diat:Door:0015" }\), 
                      \(b : diat__SmartDoor \{ uri: "urn:diat:SmartDoor:0021" }\) 
                MERGE \(a\)-\[r:ngsild__connectsTo \{ uri:"urn:ngsild:connectsTo:[a-zA-Z\-0-9]+" }]->\(b\) return a,b
            """.trimIndent()
        val door = ClassPathResource("/ngsild/door.json")
        val parsingResult = ngsiLdParserService.parseEntity(door.inputStream.readBytes().toString(Charsets.UTF_8))

        logger.debug("Cypher queries are $parsingResult")
        assertThat("urn:diat:Door:0015", equalTo(parsingResult.entityUrn))
        assertThat(parsingResult.entityStatements.size, equalTo(2))
        assertThat(parsingResult.relationshipStatements.size, equalTo(1))
        assertThat(parsingResult.relationshipStatements[0], ktMatches(expectedMatchStatement))
    }

    @Test
    fun `it should create a node with an externalized property`() {
        val expectedHasMeasureCreateStatement =
            """
                MERGE \(a:diat__hasMeasure \{ uri:"urn:diat:hasMeasure:[a-zA-Z\-0-9]+"}\)
                ON CREATE SET a = \{ value:"45", unitCode:"C", observedAt:"2019-09-26T21:32:52\+02:00", uri:"urn:diat:hasMeasure:[a-zA-Z\-0-9]+",
                                    createdAt:"[0-9\.]+", modifiedAt:"[0-9\.]+"}
                ON MATCH SET a \+= \{ value:"45", unitCode:"C", observedAt:"2019-09-26T21:32:52\+02:00", uri:"urn:diat:hasMeasure:[a-zA-Z\-0-9]+",
                                    createdAt:"[0-9\.]+", modifiedAt:"[0-9\.]+"}
                return a        
            """.trimIndent()
        val expectedObservationCreateStatement =
            """
                MERGE \(a : diat__Observation \{  uri: "urn:diat:Observation:001112"}\)
                ON CREATE SET a = \{  uri: "urn:diat:Observation:001112", createdAt: "[0-9\.]+", modifiedAt: "[0-9\.]+"}
                ON MATCH SET a \+= \{  uri: "urn:diat:Observation:001112",  createdAt: "[0-9\.]+", modifiedAt: "[0-9\.]+"}
                return a
            """.trimIndent()
        val expectedMatchStatement =
            """
                MATCH \(a : diat__Observation \{ uri: "urn:diat:Observation:001112" }\), 
                      \(b : diat__hasMeasure \{ uri: "urn:diat:hasMeasure:[a-zA-Z\-0-9]+" }\) 
                MERGE \(a\)-\[r:ngsild__hasObject \{ uri:"urn:ngsild:hasObject:[a-zA-Z\-0-9]+" }]->\(b\) return a,b
             """.trimIndent()
        val observationSensor = ClassPathResource("/ngsild/observation_sensor_prop_only.json")
        val parsingResult = ngsiLdParserService.parseEntity(observationSensor.inputStream.readBytes().toString(Charsets.UTF_8))

        assertThat("urn:diat:Observation:001112", equalTo(parsingResult.entityUrn))
        assertThat(parsingResult.entityStatements.size, equalTo(2))
        assertThat(parsingResult.relationshipStatements.size, equalTo(1))
        assertThat(parsingResult.entityStatements, hasItem(ktMatches(expectedHasMeasureCreateStatement)))
        assertThat(parsingResult.entityStatements, hasItem(ktMatches(expectedObservationCreateStatement)))
        assertThat(parsingResult.relationshipStatements[0], ktMatches(expectedMatchStatement))
    }

    @Test
    fun `check vehicle insert create certain numbers of CREATE entities and CREATE relationships`() {

        val item = ClassPathResource("/ngsild/vehicle_ngsild.json")
        val content = item.inputStream.readBytes().toString(Charsets.UTF_8)
        val ngsiLd = ngsiLdParserService.parseEntity(content)
        assertEquals(4, ngsiLd.entityStatements.size)
        assertEquals(2, ngsiLd.relationshipStatements.size)
    }

    @Test
    fun `check parking insert create certain numbers of CREATE entities and CREATE relationships`() {

        val item = ClassPathResource("/ngsild/parking_ngsild.json")
        val content = item.inputStream.readBytes().toString(Charsets.UTF_8)
        val ngsiLd = ngsiLdParserService.parseEntity(content)
        assertEquals(3, ngsiLd.entityStatements.size)
        assertEquals(2, ngsiLd.relationshipStatements.size)
    }
}
