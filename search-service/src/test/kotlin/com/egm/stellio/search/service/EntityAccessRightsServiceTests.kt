package com.egm.stellio.search.service

import com.egm.stellio.search.model.EntityAccessRights
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.test.context.ActiveProfiles
import reactor.test.StepVerifier
import java.util.UUID

@SpringBootTest
@ActiveProfiles("test")
class EntityAccessRightsServiceTests : WithTimescaleContainer {

    @Autowired
    private lateinit var entityAccessRightsService: EntityAccessRightsService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    private val subjectUuid = UUID.fromString("0768A6D5-D87B-4209-9A22-8C40A8961A79")
    private val entityId = "urn:ngsi-ld:Entity:1111".toUri()

    @AfterEach
    fun clearEntityAccessRightsTable() {
        r2dbcEntityTemplate.delete(EntityAccessRights::class.java)
            .all()
            .block()
    }

    @Test
    fun `it should add a new entity in the allowed list of read entities`() {
        StepVerifier
            .create(entityAccessRightsService.setReadRoleOnEntity(subjectUuid, entityId))
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        StepVerifier
            .create(entityAccessRightsService.hasReadRoleOnEntity(subjectUuid, "urn:ngsi-ld:Entity:1111".toUri()))
            .expectNextMatches { it == true }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should remove an entity from the allowed list of read entities`() {
        StepVerifier
            .create(entityAccessRightsService.setReadRoleOnEntity(subjectUuid, entityId))
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        StepVerifier
            .create(entityAccessRightsService.removeRoleOnEntity(subjectUuid, entityId))
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        StepVerifier
            .create(entityAccessRightsService.hasReadRoleOnEntity(subjectUuid, entityId))
            .expectNextMatches { it == false }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should find if an user has a read role on a entity`() {
        StepVerifier
            .create(entityAccessRightsService.setReadRoleOnEntity(subjectUuid, entityId))
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        StepVerifier
            .create(entityAccessRightsService.setWriteRoleOnEntity(subjectUuid, "urn:ngsi-ld:Entity:6666".toUri()))
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        StepVerifier
            .create(entityAccessRightsService.hasReadRoleOnEntity(subjectUuid, entityId))
            .expectNextMatches { it == true }
            .expectComplete()
            .verify()

        StepVerifier
            .create(entityAccessRightsService.hasReadRoleOnEntity(subjectUuid, "urn:ngsi-ld:Entity:2222".toUri()))
            .expectNextMatches { it == false }
            .expectComplete()
            .verify()

        StepVerifier
            .create(entityAccessRightsService.hasReadRoleOnEntity(subjectUuid, "urn:ngsi-ld:Entity:6666".toUri()))
            .expectNextMatches { it == true }
            .expectComplete()
            .verify()
    }
}
