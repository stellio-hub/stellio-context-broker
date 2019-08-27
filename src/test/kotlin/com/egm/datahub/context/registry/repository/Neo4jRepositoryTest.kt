package com.egm.datahub.context.registry.repository

import junit.framework.Assert.assertEquals
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.driver.v1.StatementResult
import org.springframework.core.io.ClassPathResource
import org.testcontainers.containers.Neo4jContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.MountableFile

class KtNeo4jContainer(val imageName: String) : Neo4jContainer<KtNeo4jContainer>(imageName)

@Testcontainers
class Neo4jRepositoryTest() {


    companion object {
        const val IMAGE_NAME = "neo4j"
        const val TAG_NAME = "3.5.8"



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
                //.addFileSystemBind("db","/data", BindMode.READ_WRITE)


    }
    init{
        databaseServer.start()
        val boltUrl = databaseServer.getBoltUrl()
        try {
            GraphDatabase.driver(boltUrl, AuthTokens.none()).use({ driver ->
                driver.session().use({ session ->
                    val execResult : StatementResult = session.run("CREATE INDEX ON :Resource(uri)", emptyMap<String, Any>())
                    execResult.list().map {
                        println(it)
                    }


                })
            })
        } catch (e: Exception) {
            fail(e.message)
        }

    }


    @Test
    fun `insert list of JSON-LD files successfully`() {
        val listOfFiles = listOf(
                ClassPathResource("/data/beehive.jsonld"),
                ClassPathResource("/data/beehive_not_connected.jsonld"),
                ClassPathResource("/data/beekeeper.jsonld"),
                ClassPathResource("/data/door.jsonld"),
                ClassPathResource("/data/observation_door.jsonld"),
                ClassPathResource("/data/observation_sensor.jsonld"),
                ClassPathResource("/data/sensor.jsonld"),
                ClassPathResource("/data/smartdoor.jsonld")
                )
        // Retrieve the Bolt URL from the container
        val boltUrl = databaseServer.getBoltUrl()


        try {
            GraphDatabase.driver(boltUrl, AuthTokens.none()).use({ driver ->
                driver.session().use({ session ->
                    for (item in listOfFiles) {
                        val content = item.inputStream.readBytes().toString(Charsets.UTF_8)
                        val insert = "CALL semantics.importRDFSnippet(\n" +
                                "                '$content',\n" +
                                "                'JSON-LD',\n" +
                                "                { handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500 }\n" +
                                "            )"
                        val execResult : StatementResult = session.run(insert, emptyMap<String, Any>())
                        execResult.list().map {
                            assertEquals(it.get("terminationStatus").asString(),"OK")
                            assertThat(it.get("namespaces")).isNotNull
                        }
                    }
                })
            })
        } catch (e: Exception) {
            fail(e.message)
        }

    }


    @Test
    fun queryEntitiesByLabel() {
        val beekeeper = ClassPathResource("/data/beekeeper.jsonld")
        val content = beekeeper.inputStream.readBytes().toString(Charsets.UTF_8)
        val insert = "CALL semantics.importRDFSnippet(\n" +
                "                '$content',\n" +
                "                'JSON-LD',\n" +
                "                { handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500 }\n" +
                "            )"

        val query = "MATCH (s:diat__Beekeeper) RETURN s"

        val boltUrl = databaseServer.getBoltUrl()
        try {
            GraphDatabase.driver(boltUrl, AuthTokens.none()).use({ driver ->
                driver.session().use({ session ->
                    val insertResult : StatementResult = session.run(insert, emptyMap<String, Any>())
                    insertResult.list().map {
                        assertEquals(it.get("terminationStatus").asString(),"OK")
                        assertThat(it.get("namespaces")).isNotNull
                    }

                    val execResult : StatementResult = session.run(query, emptyMap<String, Any>())
                    execResult.list().map {
                        println(it)
                    }
                })
            })
        } catch (e: Exception) {
            fail(e.message)
        }

    }




}