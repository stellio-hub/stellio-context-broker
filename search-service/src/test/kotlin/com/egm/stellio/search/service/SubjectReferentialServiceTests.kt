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
