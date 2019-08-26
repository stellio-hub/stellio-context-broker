package com.egm.datahub.context.registry.repository

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.harness.ServerControls
import org.neo4j.harness.TestServerBuilders
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.data.neo4j.Neo4jProperties
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource
import org.springframework.test.context.TestPropertySource
import semantics.RDFImport


@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(TestInstance.Lifecycle.PER_CLASS) // <1>
@TestPropertySource("classpath:application-test.properties")
class Neo4jRepositoryTest(@Autowired
                        private val neo4jProperties: Neo4jProperties) {

    private var embeddedDatabaseServer: ServerControls? = null

    @BeforeAll // <3>
    fun initializeNeo4j() {

        this.embeddedDatabaseServer = TestServerBuilders
                .newInProcessBuilder()
                .withProcedure(RDFImport::class.java) // <4>
                .withFixture("CREATE (:NamespacePrefixDefinition {\\n\" +\n" +
                        "                \"  `https://diatomic.eglobalmark.com/ontology#`: 'diat',\\n\" +\n" +
                        "                \"  `http://xmlns.com/foaf/0.1/`: 'foaf',\\n\" +\n" +
                        "                \"  `https://uri.etsi.org/ngsi-ld/v1/ontology#`: 'ngsild'})"
                )
                .newServer()
    }


    @Test
    fun insertEntity() {
        val jsonLdFile = ClassPathResource("/data/beehive.jsonld")
        val content = jsonLdFile.inputStream.readBytes().toString(Charsets.UTF_8)
        val insert = "CALL semantics.importRDFSnippet(\n" +
                "                '$content',\n" +
                "                'JSON-LD',\n" +
                "                { handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500 }\n" +
                "            )"
        GraphDatabase.driver(
                embeddedDatabaseServer?.boltURI(), AuthTokens.basic(neo4jProperties.username, neo4jProperties.password)).use({ driver ->
            driver.session().use({ session ->
                val result = session.run(insert)
                println(result)
            })
        })
    }

    @Test
    fun queryEntitiesByLabel() {
        val query = "MATCH (s:diat__Beekeeper{foaf__name: \"TEST1\"})-[r:ngsild__connectsTo]-(o ) RETURN s"
        GraphDatabase.driver(
                embeddedDatabaseServer?.boltURI(), AuthTokens.basic( neo4jProperties.username, neo4jProperties.password )).use({ driver ->
            driver.session().use({ session ->
                val result = session.run(query)
                result.list().stream().map {
                    println(it)
                }
            })
        })

        //assertThat(result).hasSize(2)
    }


}