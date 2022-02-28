package com.egm.stellio.search.service

import com.egm.stellio.search.model.EntityPayload
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.test.context.ActiveProfiles
import reactor.test.StepVerifier

@SpringBootTest
@ActiveProfiles("test")
class EntityPayloadServiceTests : WithTimescaleContainer {

    @Autowired
    private lateinit var entityPayloadService: EntityPayloadService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    private val entityUri = "urn:ngsi-ld:Entity:01".toUri()
    private val emptyJsonPayload = "{}"

    @AfterEach
    fun clearEntityAccessRightsTable() {
        r2dbcEntityTemplate.delete(EntityPayload::class.java)
            .all()
            .block()
    }

    @Test
    fun `it should create an entity payload if none existed yet`() {
        StepVerifier
            .create(entityPayloadService.createEntityPayload(entityUri, emptyJsonPayload))
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should not create an entity payload if one already existed`() {
        entityPayloadService.createEntityPayload(entityUri, emptyJsonPayload).block()

        StepVerifier
            .create(entityPayloadService.createEntityPayload(entityUri, emptyJsonPayload))
            .expectErrorMatches { it is DataIntegrityViolationException }
            .verify()
    }

    @Test
    fun `it should upsert an entity payload if one already existed`() {
        entityPayloadService.createEntityPayload(entityUri, emptyJsonPayload).block()

        StepVerifier
            .create(entityPayloadService.upsertEntityPayload(entityUri, emptyJsonPayload))
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should delete an entity payload`() {
        entityPayloadService.createEntityPayload(entityUri, emptyJsonPayload).block()

        StepVerifier
            .create(entityPayloadService.deleteEntityPayload(entityUri))
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        // if correctly deleted, we should be able to create a new one
        StepVerifier
            .create(entityPayloadService.createEntityPayload(entityUri, emptyJsonPayload))
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()
    }
}
