package com.egm.stellio.search.temporal.service

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.authorization.service.AuthorizationService
import com.egm.stellio.search.entity.service.EntityQueryService
import com.egm.stellio.search.entity.service.EntityService
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.INCOMING_IRI
import com.egm.stellio.shared.util.loadAndExpandSampleData
import com.egm.stellio.shared.util.shouldSucceed
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [TemporalService::class])
@ActiveProfiles("test")
@ExperimentalCoroutinesApi
class TemporalServiceTests {

    @Autowired
    private lateinit var temporalService: TemporalService

    @MockkBean
    private lateinit var entityQueryService: EntityQueryService

    @MockkBean
    private lateinit var entityService: EntityService

    @MockkBean
    private lateinit var attributeInstanceService: AttributeInstanceService

    @MockkBean
    private lateinit var authorizationService: AuthorizationService

    private val entityUri = "urn:ngsi-ld:BeeHive:TESTC".toUri()

    private fun mockkAuthorizationForCreation() {
        coEvery { authorizationService.userCanCreateEntities() } returns Unit.right()
        coEvery { authorizationService.userCanUpdateEntity(any()) } returns Unit.right()
        coEvery { authorizationService.createOwnerRight(any()) } returns Unit.right()
    }

    @Test
    fun `it should ask to create a temporal entity if it does not exist yet`() = runTest {
        mockkAuthorizationForCreation()
        coEvery {
            entityQueryService.isMarkedAsDeleted(entityUri)
        } returns ResourceNotFoundException("Entity does not exist").left()
        coEvery { entityService.createEntity(any(), any()) } returns Unit.right()
        coEvery { entityService.upsertAttributes(any(), any()) } returns Unit.right()

        val expandedEntity = loadAndExpandSampleData("temporal/beehive_create_temporal_entity.jsonld")

        temporalService.createOrUpdateTemporalEntity(entityUri, expandedEntity).shouldSucceed()
    }

    @Test
    fun `it should ask to create a temporal entity if it already exists but is deleted`() = runTest {
        mockkAuthorizationForCreation()
        coEvery { entityQueryService.isMarkedAsDeleted(entityUri) } returns false.right()
        coEvery { entityService.createEntity(any(), any()) } returns Unit.right()
        coEvery { entityService.upsertAttributes(any(), any()) } returns Unit.right()

        val expandedEntity = loadAndExpandSampleData("temporal/beehive_create_temporal_entity.jsonld")

        temporalService.createOrUpdateTemporalEntity(entityUri, expandedEntity).shouldSucceed()
    }

    @Test
    fun `it should ask to upsert a temporal entity if it already exists but is not deleted`() = runTest {
        mockkAuthorizationForCreation()
        coEvery { entityQueryService.isMarkedAsDeleted(entityUri) } returns false.right()
        coEvery { entityService.upsertAttributes(any(), any()) } returns Unit.right()

        val expandedEntity = loadAndExpandSampleData("temporal/beehive_create_temporal_entity.jsonld")

        temporalService.createOrUpdateTemporalEntity(entityUri, expandedEntity).shouldSucceed()
    }

    @Test
    fun `it should ask to permanently delete a temporal entity`() = runTest {
        coEvery { entityService.permanentlyDeleteEntity(any(), any()) } returns Unit.right()

        temporalService.deleteEntity(entityUri).shouldSucceed()

        coVerify(exactly = 1) {
            entityService.permanentlyDeleteEntity(entityUri)
        }
    }

    @Test
    fun `it should ask to permanently delete a temporal attribute`() = runTest {
        coEvery { entityService.permanentlyDeleteAttribute(any(), any(), any(), any()) } returns Unit.right()

        temporalService.deleteAttribute(entityUri, INCOMING_IRI, null).shouldSucceed()

        coVerify(exactly = 1) {
            entityService.permanentlyDeleteAttribute(entityUri, INCOMING_IRI, null)
        }
    }

    @Test
    fun `it should ask to permanently delete a temporal attribute instance`() = runTest {
        val instanceId = "urn:ngsi-ld:Instance:01".toUri()

        coEvery { entityQueryService.checkEntityExistence(entityUri) } returns Unit.right()
        coEvery { authorizationService.userCanUpdateEntity(entityUri) } returns Unit.right()
        coEvery {
            attributeInstanceService.deleteInstance(entityUri, INCOMING_IRI, instanceId)
        } returns Unit.right()

        temporalService.deleteAttributeInstance(entityUri, INCOMING_IRI, instanceId).shouldSucceed()

        coVerify(exactly = 1) {
            attributeInstanceService.deleteInstance(entityUri, INCOMING_IRI, instanceId)
        }
    }
}
