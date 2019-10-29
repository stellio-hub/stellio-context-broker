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

    val ISO8601_REGEXP = "(?:[1-9]\\d{3}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1\\d|2[0-8])|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)|(?:[1-9]\\d(?:0[48]|[2468][048]|[13579][26])|(?:[2468][048]|[13579][26])00)-02-29)T(?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d(?:\\.\\d{1,9})?(?:Z|[+-][01]\\d:[0-5]\\d)"
    @Autowired
    private lateinit var ngsiLdParserService: NgsiLdParserService

    private val logger = LoggerFactory.getLogger(NgsiLdParserServiceTests::class.java)

    @Test
    fun `it should create a simple node with simple properties`() {
        val expectedCreateStatement =
            """
                MERGE \(a : diat__Beekeeper \{ uri: "urn:diat:Beekeeper:Pascal" }\)
                ON CREATE SET a = \{ name: "Scalpa", uri: "urn:diat:Beekeeper:Pascal", createdAt: "$ISO8601_REGEXP", modifiedAt: "$ISO8601_REGEXP" }
                ON MATCH SET a \+= \{ name: "Scalpa", uri: "urn:diat:Beekeeper:Pascal", createdAt: "$ISO8601_REGEXP", modifiedAt: "$ISO8601_REGEXP" }
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
                    uri: "urn:diat:BeeHive:TESTC", createdAt: "$ISO8601_REGEXP", modifiedAt: "$ISO8601_REGEXP"}
                ON MATCH  SET a \+= \{ name: "ParisBeehive12", location: "point\(\{ x: 13.3986 , y: 52.5547, crs: \\u0027WGS-84\\u0027 }\)", 
                    uri: "urn:diat:BeeHive:TESTC", createdAt: "$ISO8601_REGEXP", modifiedAt: "$ISO8601_REGEXP"}
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
                                    createdAt:"$ISO8601_REGEXP", modifiedAt:"$ISO8601_REGEXP"}
                ON MATCH SET a \+= \{ value:"45", unitCode:"C", observedAt:"2019-09-26T21:32:52\+02:00", uri:"urn:diat:hasMeasure:[a-zA-Z\-0-9]+",
                                    createdAt:"$ISO8601_REGEXP", modifiedAt:"$ISO8601_REGEXP"}
                return a        
            """.trimIndent()
        val expectedObservationCreateStatement =
            """
                MERGE \(a : sosa__Observation \{  uri: "urn:sosa:Observation:001112"}\)
                ON CREATE SET a = \{  uri: "urn:sosa:Observation:001112", createdAt: "$ISO8601_REGEXP", modifiedAt: "$ISO8601_REGEXP"}
                ON MATCH SET a \+= \{  uri: "urn:sosa:Observation:001112",  createdAt: "$ISO8601_REGEXP", modifiedAt: "$ISO8601_REGEXP"}
                return a
            """.trimIndent()
        val expectedMatchStatement =
            """
                MATCH \(a : sosa__Observation \{ uri: "urn:sosa:Observation:001112" }\), 
                      \(b : diat__hasMeasure \{ uri: "urn:diat:hasMeasure:[a-zA-Z\-0-9]+" }\) 
                MERGE \(a\)-\[r:ngsild__hasValue \{ uri:"urn:ngsild:hasValue:[a-zA-Z\-0-9]+" }]->\(b\) return a,b
             """.trimIndent()
        val observationSensor = ClassPathResource("/ngsild/observation_sensor_prop_only.json")
        val parsingResult = ngsiLdParserService.parseEntity(observationSensor.inputStream.readBytes().toString(Charsets.UTF_8))

        assertThat("urn:sosa:Observation:001112", equalTo(parsingResult.entityUrn))
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
