package com.egm.stellio.search.temporal.service

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.authorization.service.AuthorizationService
import com.egm.stellio.search.entity.service.EntityQueryService
import com.egm.stellio.search.entity.service.EntityService
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.INCOMING_PROPERTY
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
    private val sub = "0123456789-1234-5678-987654321"

    private fun mockkAuthorizationForCreation() {
        coEvery { authorizationService.userCanCreateEntities(any()) } returns Unit.right()
        coEvery { authorizationService.userCanUpdateEntity(any(), any()) } returns Unit.right()
        coEvery { authorizationService.createOwnerRight(any(), any()) } returns Unit.right()
    }

    @Test
    fun `it should ask to create a temporal entity if it does not exist yet`() = runTest {
        mockkAuthorizationForCreation()
        coEvery {
            entityQueryService.isMarkedAsDeleted(entityUri)
        } returns ResourceNotFoundException("Entity does not exist").left()
        coEvery { entityService.createEntity(any(), any(), any()) } returns Unit.right()
        coEvery { entityService.upsertAttributes(any(), any(), any()) } returns Unit.right()

        val expandedEntity = loadAndExpandSampleData("temporal/beehive_create_temporal_entity.jsonld")

        temporalService.createOrUpdateTemporalEntity(entityUri, expandedEntity, sub).shouldSucceed()
    }

    @Test
    fun `it should ask to create a temporal entity if it already exists but is deleted`() = runTest {
        mockkAuthorizationForCreation()
        coEvery { entityQueryService.isMarkedAsDeleted(entityUri) } returns false.right()
        coEvery { entityService.createEntity(any(), any(), any()) } returns Unit.right()
        coEvery { entityService.upsertAttributes(any(), any(), any()) } returns Unit.right()

        val expandedEntity = loadAndExpandSampleData("temporal/beehive_create_temporal_entity.jsonld")

        temporalService.createOrUpdateTemporalEntity(entityUri, expandedEntity, sub).shouldSucceed()
    }

    @Test
    fun `it should ask to upsert a temporal entity if it already exists but is not deleted`() = runTest {
        mockkAuthorizationForCreation()
        coEvery { entityQueryService.isMarkedAsDeleted(entityUri) } returns false.right()
        coEvery { entityService.upsertAttributes(any(), any(), any()) } returns Unit.right()

        val expandedEntity = loadAndExpandSampleData("temporal/beehive_create_temporal_entity.jsonld")

        temporalService.createOrUpdateTemporalEntity(entityUri, expandedEntity, sub).shouldSucceed()
    }

    @Test
    fun `it should ask to permanently delete a temporal entity`() = runTest {
        coEvery { entityService.permanentlyDeleteEntity(any(), any()) } returns Unit.right()

        temporalService.deleteEntity(entityUri, sub).shouldSucceed()

        coVerify(exactly = 1) {
            entityService.permanentlyDeleteEntity(entityUri, sub)
        }
    }

    @Test
    fun `it should ask to permanently delete a temporal attribute`() = runTest {
        coEvery { entityService.permanentlyDeleteAttribute(any(), any(), any(), any(), any()) } returns Unit.right()

        temporalService.deleteAttribute(entityUri, INCOMING_PROPERTY, null).shouldSucceed()

        coVerify(exactly = 1) {
            entityService.permanentlyDeleteAttribute(entityUri, INCOMING_PROPERTY, null)
        }
    }

    @Test
    fun `it should ask to permanently delete a temporal attribute instance`() = runTest {
        val instanceId = "urn:ngsi-ld:Instance:01".toUri()

        coEvery { entityQueryService.checkEntityExistence(entityUri) } returns Unit.right()
        coEvery { authorizationService.userCanUpdateEntity(entityUri, any()) } returns Unit.right()
        coEvery {
            attributeInstanceService.deleteInstance(entityUri, INCOMING_PROPERTY, instanceId)
        } returns Unit.right()

        temporalService.deleteAttributeInstance(entityUri, INCOMING_PROPERTY, instanceId).shouldSucceed()

        coVerify(exactly = 1) {
            attributeInstanceService.deleteInstance(entityUri, INCOMING_PROPERTY, instanceId)
        }
    }
}
