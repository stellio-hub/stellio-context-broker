package com.egm.datahub.context.search.service

import com.egm.datahub.context.search.loadAndParseSampleData
import org.flywaydb.core.Flyway
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.ActiveProfiles
import reactor.test.StepVerifier

@SpringBootTest
@ActiveProfiles("test")
@Import(R2DBCConfiguration::class)
class EntityServiceTests {

    @Autowired
    private lateinit var entityService: EntityService

    init {
        Flyway.configure()
            .dataSource(MyPostgresqlContainer.instance.jdbcUrl, MyPostgresqlContainer.DB_USER, MyPostgresqlContainer.DB_PASSWORD)
            .load()
            .migrate()
    }

    @Test
    fun `it should create one entry for an entity with one temporal property`() {
        val rawEntity = loadAndParseSampleData()

        val temporalReferencesResults = entityService.createEntityTemporalReferences(rawEntity)

        StepVerifier.create(temporalReferencesResults)
            .expectNextMatches {
                it == 1
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should create two entries for an entity with two temporal properties`() {
        val rawEntity = loadAndParseSampleData("beehive_two_temporal_properties.jsonld")

        val temporalReferencesResults = entityService.createEntityTemporalReferences(rawEntity)

        StepVerifier.create(temporalReferencesResults)
            .expectNextMatches {
                it == 2
            }
            .expectComplete()
            .verify()
    }
}