package com.egm.stellio.search.service

import com.egm.stellio.search.model.EntityAccessRights
import com.egm.stellio.search.model.SubjectReferential
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.support.WithKafkaContainer
import com.egm.stellio.shared.util.SubjectType
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.Called
import io.mockk.every
import io.mockk.mockkClass
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.test.context.ActiveProfiles
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.util.UUID

@SpringBootTest
@ActiveProfiles("test")
class EntityAccessRightsServiceTests : WithTimescaleContainer, WithKafkaContainer {

    @Autowired
    private lateinit var entityAccessRightsService: EntityAccessRightsService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    @MockkBean(relaxed = true)
    private lateinit var subjectReferentialService: SubjectReferentialService

    private val subjectUuid = UUID.fromString("0768A6D5-D87B-4209-9A22-8C40A8961A79")
    private val groupUuid = UUID.fromString("220FC854-3609-404B-BC77-F2DFE332B27B")
    private val entityId = "urn:ngsi-ld:Entity:1111".toUri()
    private val defaultMockSubjectReferential = mockkClass(SubjectReferential::class)

    @BeforeEach
    fun setDefaultBehaviorOnSubjectReferential() {
        every { subjectReferentialService.hasStellioAdminRole(subjectUuid) } answers { Mono.just(false) }
        every { subjectReferentialService.retrieve(subjectUuid) } answers { Mono.just(defaultMockSubjectReferential) }
        every { defaultMockSubjectReferential.groupsMemberships } returns emptyList()
    }

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
            .create(entityAccessRightsService.canReadEntity(subjectUuid, "urn:ngsi-ld:Entity:1111".toUri()))
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
            .create(entityAccessRightsService.canReadEntity(subjectUuid, entityId))
            .expectNextMatches { it == false }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should allow an user having a direct read role on a entity`() {
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
            .create(entityAccessRightsService.canReadEntity(subjectUuid, entityId))
            .expectNextMatches { it == true }
            .expectComplete()
            .verify()

        StepVerifier
            .create(entityAccessRightsService.canReadEntity(subjectUuid, "urn:ngsi-ld:Entity:2222".toUri()))
            .expectNextMatches { it == false }
            .expectComplete()
            .verify()

        StepVerifier
            .create(entityAccessRightsService.canWriteEntity(subjectUuid, "urn:ngsi-ld:Entity:6666".toUri()))
            .expectNextMatches { it == true }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should allow an user having a read role on a entity via a group membership`() {
        every {
            subjectReferentialService.retrieve(subjectUuid)
        } answers {
            Mono.just(
                SubjectReferential(
                    subjectId = subjectUuid,
                    subjectType = SubjectType.USER,
                    groupsMemberships = listOf(groupUuid, UUID.randomUUID())
                )
            )
        }

        entityAccessRightsService.setReadRoleOnEntity(groupUuid, entityId).block()

        StepVerifier
            .create(entityAccessRightsService.canReadEntity(subjectUuid, entityId))
            .expectNextMatches { it == true }
            .expectComplete()
            .verify()

        StepVerifier
            .create(entityAccessRightsService.canReadEntity(subjectUuid, "urn:ngsi-ld:Entity:2222".toUri()))
            .expectNextMatches { it == false }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should allow an user having a read role on a entity both directly and via a group membership`() {
        every {
            subjectReferentialService.retrieve(subjectUuid)
        } answers {
            Mono.just(
                SubjectReferential(
                    subjectId = subjectUuid,
                    subjectType = SubjectType.USER,
                    groupsMemberships = listOf(groupUuid, UUID.randomUUID())
                )
            )
        }

        entityAccessRightsService.setReadRoleOnEntity(groupUuid, entityId).block()
        entityAccessRightsService.setReadRoleOnEntity(subjectUuid, entityId).block()

        StepVerifier
            .create(entityAccessRightsService.canReadEntity(subjectUuid, entityId))
            .expectNextMatches { it == true }
            .expectComplete()
            .verify()

        StepVerifier
            .create(entityAccessRightsService.canReadEntity(subjectUuid, "urn:ngsi-ld:Entity:2222".toUri()))
            .expectNextMatches { it == false }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should allow an user having the stellio-admin role to read any entity`() {
        every { subjectReferentialService.hasStellioAdminRole(subjectUuid) } answers { Mono.just(true) }

        StepVerifier
            .create(entityAccessRightsService.canReadEntity(subjectUuid, entityId))
            .expectNextMatches { it == true }
            .expectComplete()
            .verify()

        StepVerifier
            .create(entityAccessRightsService.canReadEntity(subjectUuid, "urn:ngsi-ld:Entity:2222".toUri()))
            .expectNextMatches { it == true }
            .expectComplete()
            .verify()

        verify { subjectReferentialService.retrieve(eq(subjectUuid)) wasNot Called }
    }
}
