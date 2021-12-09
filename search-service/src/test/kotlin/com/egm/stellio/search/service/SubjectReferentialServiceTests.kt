package com.egm.stellio.search.service

import com.egm.stellio.search.model.SubjectReferential
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.util.GlobalRole
import com.egm.stellio.shared.util.SubjectType
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
class SubjectReferentialServiceTests : WithTimescaleContainer {

    @Autowired
    private lateinit var subjectReferentialService: SubjectReferentialService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    private val subjectUuid = UUID.fromString("0768A6D5-D87B-4209-9A22-8C40A8961A79")
    private val groupUuid = UUID.fromString("52A916AB-19E6-4D3B-B629-936BC8E5B640")

    @AfterEach
    fun clearSubjectReferentialTable() {
        r2dbcEntityTemplate.delete(SubjectReferential::class.java)
            .all()
            .block()
    }

    @Test
    fun `it should persist a subject referential`() {
        val subjectReferential = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            globalRoles = listOf(GlobalRole.STELLIO_ADMIN)
        )

        StepVerifier
            .create(subjectReferentialService.create(subjectReferential))
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should retrieve a subject referential`() {
        val subjectReferential = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            globalRoles = listOf(GlobalRole.STELLIO_ADMIN)
        )

        subjectReferentialService.create(subjectReferential).block()

        StepVerifier
            .create(subjectReferentialService.retrieve(subjectUuid))
            .expectNextMatches {
                it.subjectId == subjectUuid &&
                    it.subjectType == SubjectType.USER &&
                    it.globalRoles == listOf(GlobalRole.STELLIO_ADMIN)
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should update the global role of a subject`() {
        val subjectReferential = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER
        )

        subjectReferentialService.create(subjectReferential).block()

        StepVerifier
            .create(subjectReferentialService.setGlobalRoles(subjectUuid, listOf(GlobalRole.STELLIO_ADMIN)))
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        StepVerifier
            .create(subjectReferentialService.retrieve(subjectUuid))
            .expectNextMatches {
                it.globalRoles == listOf(GlobalRole.STELLIO_ADMIN)
            }
            .expectComplete()
            .verify()

        StepVerifier
            .create(subjectReferentialService.resetGlobalRoles(subjectUuid))
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        StepVerifier
            .create(subjectReferentialService.retrieve(subjectUuid))
            .expectNextMatches {
                it.globalRoles == null
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should add a group membership to an user`() {
        val userAccessRights = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER
        )

        subjectReferentialService.create(userAccessRights).block()

        StepVerifier
            .create(subjectReferentialService.addGroupMembershipToUser(subjectUuid, groupUuid))
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        StepVerifier
            .create(subjectReferentialService.retrieve(subjectUuid))
            .expectNextMatches {
                it.groupsMemberships == listOf(groupUuid)
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should add a group membership to an user inside an existing list`() {
        val userAccessRights = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER
        )

        subjectReferentialService.create(userAccessRights).block()
        subjectReferentialService.addGroupMembershipToUser(subjectUuid, groupUuid).block()

        val newGroupUuid = UUID.randomUUID()
        StepVerifier
            .create(subjectReferentialService.addGroupMembershipToUser(subjectUuid, newGroupUuid))
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        StepVerifier
            .create(subjectReferentialService.retrieve(subjectUuid))
            .expectNextMatches {
                it.groupsMemberships == listOf(groupUuid, newGroupUuid)
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should remove a group membership to an user`() {
        val userAccessRights = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER
        )

        subjectReferentialService.create(userAccessRights).block()
        subjectReferentialService.addGroupMembershipToUser(subjectUuid, groupUuid).block()

        StepVerifier
            .create(subjectReferentialService.removeGroupMembershipToUser(subjectUuid, groupUuid))
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        StepVerifier
            .create(subjectReferentialService.retrieve(subjectUuid))
            .expectNextMatches {
                it.groupsMemberships?.isEmpty() ?: false
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should add a service account id to a client`() {
        val userAccessRights = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER
        )

        subjectReferentialService.create(userAccessRights).block()

        val serviceAccountId = UUID.randomUUID()
        StepVerifier
            .create(subjectReferentialService.addServiceAccountIdToClient(subjectUuid, serviceAccountId))
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        StepVerifier
            .create(subjectReferentialService.retrieve(subjectUuid))
            .expectNextMatches {
                it.serviceAccountId == serviceAccountId
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should delete a subject referential`() {
        val userAccessRights = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            globalRoles = listOf(GlobalRole.STELLIO_ADMIN)
        )

        subjectReferentialService.create(userAccessRights).block()

        StepVerifier
            .create(subjectReferentialService.delete(subjectUuid))
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        StepVerifier
            .create(subjectReferentialService.retrieve(subjectUuid))
            .expectComplete()
            .verify()
    }
}
