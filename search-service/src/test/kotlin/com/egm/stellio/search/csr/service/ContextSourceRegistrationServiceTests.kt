package com.egm.stellio.search.csr.service

import arrow.core.Some
import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.search.csr.model.ContextSourceRegistration.Companion.notFoundMessage
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.shouldFailWith
import com.egm.stellio.shared.util.shouldSucceed
import com.egm.stellio.shared.util.shouldSucceedAndResult
import com.egm.stellio.shared.util.shouldSucceedWith
import com.egm.stellio.shared.util.toUri
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import java.util.UUID

@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = ["application.authentication.enabled=false"])
class ContextSourceRegistrationServiceTests : WithTimescaleContainer {

    @Autowired
    private lateinit var contextSourceRegistrationService: ContextSourceRegistrationService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    private val mockUserSub = Some(UUID.randomUUID().toString())

    @AfterEach
    fun deleteContextSourceRegistrations() {
        r2dbcEntityTemplate.delete(ContextSourceRegistration::class.java)
            .all()
            .block()
    }

    fun loadAndDeserializeContextSourceRegistration(
        filename: String,
        contexts: List<String> = APIC_COMPOUND_CONTEXTS
    ): ContextSourceRegistration {
        val csrPayload = loadSampleData(filename)
        return ContextSourceRegistration.deserialize(csrPayload.deserializeAsMap(), contexts)
            .shouldSucceedAndResult()
    }

    @Test
    fun `creating a second CSR with the same id should fail with AlreadyExistError`() = runTest {
        val contextSourceRegistration =
            loadAndDeserializeContextSourceRegistration("csr/contextSourceRegistration_minimal_entities.json")
        contextSourceRegistrationService.create(contextSourceRegistration, mockUserSub).shouldSucceed()
        contextSourceRegistrationService.create(contextSourceRegistration, mockUserSub).shouldFailWith {
            it is AlreadyExistsException
        }
    }

    @Test
    fun `getting a simple CSR should return the created contextSourceRegistration`() = runTest {
        val contextSourceRegistration =
            loadAndDeserializeContextSourceRegistration("csr/contextSourceRegistration_minimal_entities.json")
        contextSourceRegistrationService.create(contextSourceRegistration, mockUserSub).shouldSucceed()

        contextSourceRegistrationService.getById(
            contextSourceRegistration.id
        ).shouldSucceedWith {
            assertEquals(it, contextSourceRegistration)
        }
    }

    @Test
    fun `getting a full CSR should return the created CSR`() = runTest {
        val contextSourceRegistration =
            loadAndDeserializeContextSourceRegistration("csr/contextSourceRegistration_full.json")
        contextSourceRegistrationService.create(contextSourceRegistration, mockUserSub).shouldSucceed()

        contextSourceRegistrationService.getById(
            contextSourceRegistration.id
        ).shouldSucceedWith {
            assertEquals(it, contextSourceRegistration)
        }
    }

    @Test
    fun `deleting an existing CSR should succeed`() = runTest {
        val contextSourceRegistration =
            loadAndDeserializeContextSourceRegistration("csr/contextSourceRegistration_minimal_entities.json")
        contextSourceRegistrationService.create(contextSourceRegistration, mockUserSub).shouldSucceed()

        contextSourceRegistrationService.delete(contextSourceRegistration.id).shouldSucceed()

        contextSourceRegistrationService.getById(contextSourceRegistration.id).shouldFailWith {
            it is ResourceNotFoundException &&
                it.message == notFoundMessage(contextSourceRegistration.id)
        }
    }

    @Test
    fun `deletin an non existing CSR should return a RessourceNotFound Error`() = runTest {
        val id = "urn:ngsi-ld:ContextSourceRegistration:UnknownContextSourceRegistration".toUri()
        contextSourceRegistrationService.delete(
            id
        ).shouldFailWith {
            it is ResourceNotFoundException &&
                it.message == notFoundMessage(id)
        }
    }
}
