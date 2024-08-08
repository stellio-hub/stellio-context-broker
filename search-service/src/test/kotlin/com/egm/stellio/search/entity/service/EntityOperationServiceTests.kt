package com.egm.stellio.search.entity.service

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.authorization.service.AuthorizationService
import com.egm.stellio.search.entity.model.EMPTY_UPDATE_RESULT
import com.egm.stellio.search.entity.model.EntityPayload
import com.egm.stellio.search.entity.model.NotUpdatedDetails
import com.egm.stellio.search.entity.model.UpdateResult
import com.egm.stellio.search.entity.web.BatchEntityError
import com.egm.stellio.search.entity.web.BatchEntitySuccess
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.InternalErrorException
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import kotlinx.coroutines.Job
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [EntityOperationService::class])
@ActiveProfiles("test")
class EntityOperationServiceTests {

    @MockkBean
    private lateinit var entityService: EntityService

    @MockkBean(relaxed = true)
    private lateinit var entityAttributeService: EntityAttributeService

    @MockkBean
    private lateinit var authorizationService: AuthorizationService

    @MockkBean
    private lateinit var entityEventService: EntityEventService

    @Autowired
    private lateinit var entityOperationService: EntityOperationService

    val firstEntityURI = "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
    val secondEntityURI = "urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()
    private lateinit var firstExpandedEntity: ExpandedEntity
    private lateinit var firstEntity: NgsiLdEntity
    private lateinit var secondExpandedEntity: ExpandedEntity
    private lateinit var secondEntity: NgsiLdEntity

    val sub: Sub = "60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"

    @BeforeEach
    fun initNgsiLdEntitiesMocks() {
        firstExpandedEntity = mockkClass(ExpandedEntity::class, relaxed = true) {
            every { id } returns firstEntityURI.toString()
            every { members } returns emptyMap()
        }
        firstEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { firstEntity.id } returns firstEntityURI
        secondExpandedEntity = mockkClass(ExpandedEntity::class, relaxed = true) {
            every { id } returns secondEntityURI.toString()
            every { members } returns emptyMap()
        }
        secondEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { secondEntity.id } returns secondEntityURI
    }

    @Test
    fun `splitEntitiesByExistence should split entities per existence`() = runTest {
        coEvery {
            entityService.filterExistingEntitiesAsIds(listOf(firstEntityURI, secondEntityURI))
        } returns listOf(firstEntityURI)

        val (exist, doNotExist) = entityOperationService.splitEntitiesByExistence(
            listOf(
                Pair(firstExpandedEntity, firstEntity),
                Pair(secondExpandedEntity, secondEntity)
            )
        )

        assertEquals(listOf(Pair(firstExpandedEntity, firstEntity)), exist)
        assertEquals(listOf(Pair(secondExpandedEntity, secondEntity)), doNotExist)
    }

    @Test
    fun `splitEntitiesByExistence should split entities per existence with ids`() = runTest {
        coEvery {
            entityService.filterExistingEntitiesAsIds(listOf(firstEntityURI, secondEntityURI))
        } returns listOf(firstEntityURI)

        val (exist, doNotExist) =
            entityOperationService.splitEntitiesIdsByExistence(listOf(firstEntityURI, secondEntityURI))

        assertEquals(listOf(firstEntityURI), exist)
        assertEquals(listOf(secondEntityURI), doNotExist)
    }

    @Test
    fun `splitEntitiesByUniqueness should split entities per uniqueness`() = runTest {
        val (unique, duplicates) = entityOperationService.splitEntitiesByUniqueness(
            listOf(
                Pair(firstExpandedEntity, firstEntity),
                Pair(secondExpandedEntity, secondEntity),
                Pair(firstExpandedEntity, firstEntity),
            )
        )

        assertEquals(listOf(Pair(firstExpandedEntity, firstEntity), Pair(secondExpandedEntity, secondEntity)), unique)
        assertEquals(listOf(Pair(firstExpandedEntity, firstEntity)), duplicates)
    }

    @Test
    fun `splitEntitiesIdsByUniqueness should split entities per uniqueness with ids`() = runTest {
        val (unique, duplicates) =
            entityOperationService.splitEntitiesIdsByUniqueness(listOf(firstEntityURI, secondEntityURI, firstEntityURI))

        assertEquals(listOf(firstEntityURI, secondEntityURI), unique)
        assertEquals(listOf(firstEntityURI), duplicates)
    }

    @Test
    fun `processEntities should count as error a process which raises a BadRequestDataException`() = runTest {
        coEvery {
            entityService.appendAttributes(firstEntityURI, any(), any(), any())
        } returns EMPTY_UPDATE_RESULT.right()
        coEvery {
            entityService.appendAttributes(secondEntityURI, any(), any(), any())
        } returns BadRequestDataException("error").left()
        coEvery { entityEventService.publishAttributeChangeEvents(any(), any(), any(), any(), any()) } returns Job()

        val batchOperationResult =
            entityOperationService.processEntities(
                listOf(
                    firstExpandedEntity to firstEntity,
                    secondExpandedEntity to secondEntity
                ),
                false,
                sub,
                entityOperationService::updateEntity
            )

        assertEquals(
            listOf(BatchEntitySuccess(firstEntityURI, updateResult = EMPTY_UPDATE_RESULT)),
            batchOperationResult.success
        )
        assertEquals(
            listOf(BatchEntityError(secondEntityURI, arrayListOf("error"))),
            batchOperationResult.errors
        )
        coVerify {
            entityEventService.publishAttributeChangeEvents(
                sub,
                eq(firstEntityURI),
                any(),
                match { it.isSuccessful() },
                any()
            )
        }
        coVerify {
            entityEventService.publishAttributeChangeEvents(
                sub,
                eq(secondEntityURI),
                any(),
                any(),
                any()
            ) wasNot Called
        }
    }

    @Test
    fun `processEntities should count as error not processed attributes in entities`() = runTest {
        val updateResult = UpdateResult(
            emptyList(),
            listOf(
                NotUpdatedDetails("attribute#1", "reason"),
                NotUpdatedDetails("attribute#2", "reason")
            )
        )
        coEvery {
            entityService.appendAttributes(firstEntityURI, any(), any(), any())
        } returns EMPTY_UPDATE_RESULT.right()
        coEvery {
            entityService.appendAttributes(secondEntityURI, any(), any(), any())
        } returns updateResult.right()
        coEvery { entityEventService.publishAttributeChangeEvents(any(), any(), any(), any(), any()) } returns Job()

        val batchOperationResult = entityOperationService.processEntities(
            listOf(
                firstExpandedEntity to firstEntity,
                secondExpandedEntity to secondEntity
            ),
            false,
            sub,
            entityOperationService::updateEntity
        )

        assertEquals(
            listOf(BatchEntitySuccess(firstEntityURI, EMPTY_UPDATE_RESULT)),
            batchOperationResult.success
        )
        assertEquals(
            listOf(
                BatchEntityError(
                    secondEntityURI,
                    arrayListOf("attribute#1 : reason, attribute#2 : reason")
                )
            ),
            batchOperationResult.errors
        )
    }

    @Test
    fun `batch create should ask to create all provided entities`() = runTest {
        coEvery { entityService.createEntity(any<NgsiLdEntity>(), any(), any()) } returns Unit.right()
        coEvery { entityEventService.publishEntityCreateEvent(any(), any(), any()) } returns Job()

        val batchOperationResult = entityOperationService.create(
            listOf(
                Pair(firstExpandedEntity, firstEntity),
                Pair(secondExpandedEntity, secondEntity)
            ),
            sub
        )

        assertEquals(
            arrayListOf(firstEntityURI, secondEntityURI),
            batchOperationResult.getSuccessfulEntitiesIds()
        )
        assertTrue(batchOperationResult.errors.isEmpty())

        coVerify {
            entityService.createEntity(firstEntity, firstExpandedEntity, sub)
        }
        coVerify {
            entityService.createEntity(secondEntity, secondExpandedEntity, sub)
        }
        coVerify(exactly = 2) {
            entityEventService.publishEntityCreateEvent(any(), any(), any())
        }
    }

    @Test
    fun `batch create should ask to create entities and transmit back any error`() = runTest {
        coEvery { entityService.createEntity(firstEntity, any(), any()) } returns Unit.right()
        coEvery {
            entityService.createEntity(secondEntity, any(), any())
        } returns BadRequestDataException("Invalid entity").left()
        coEvery { entityEventService.publishEntityCreateEvent(any(), any(), any()) } returns Job()

        val batchOperationResult = entityOperationService.create(
            listOf(
                Pair(firstExpandedEntity, firstEntity),
                Pair(secondExpandedEntity, secondEntity)
            ),
            sub
        )

        assertEquals(arrayListOf(BatchEntitySuccess(firstEntityURI)), batchOperationResult.success)
        assertEquals(
            arrayListOf(
                BatchEntityError(secondEntityURI, arrayListOf("Invalid entity"))
            ),
            batchOperationResult.errors
        )
        coVerify(exactly = 1) {
            entityEventService.publishEntityCreateEvent(any(), any(), any())
        }
    }

    @Test
    fun `batch update should ask to update attributes of entities`() = runTest {
        coEvery {
            entityService.appendAttributes(any(), any(), any(), any())
        } returns EMPTY_UPDATE_RESULT.right()
        coEvery { entityEventService.publishAttributeChangeEvents(any(), any(), any(), any(), any()) } returns Job()

        val batchOperationResult = entityOperationService.update(
            listOf(
                Pair(firstExpandedEntity, firstEntity),
                Pair(secondExpandedEntity, secondEntity)
            ),
            false,
            sub
        )

        assertEquals(
            listOf(firstEntityURI, secondEntityURI),
            batchOperationResult.getSuccessfulEntitiesIds()
        )

        coVerify {
            entityService.appendAttributes(eq(firstEntityURI), any(), false, sub)
        }
        coVerify {
            entityService.appendAttributes(eq(secondEntityURI), any(), false, sub)
        }
        coVerify(exactly = 2) {
            entityEventService.publishAttributeChangeEvents(any(), any(), any(), any(), any())
        }
    }

    @Test
    fun `batch replace should ask to replace entities`() = runTest {
        coEvery {
            entityAttributeService.deleteTemporalAttributesOfEntity(any())
        } returns Unit.right()
        coEvery {
            entityService.appendAttributes(any(), any(), any(), any())
        } returns EMPTY_UPDATE_RESULT.right()
        coEvery { entityEventService.publishEntityReplaceEvent(any(), any(), any()) } returns Job()

        val batchOperationResult = entityOperationService.replace(
            listOf(
                Pair(firstExpandedEntity, firstEntity),
                Pair(secondExpandedEntity, secondEntity)
            ),
            sub
        )

        assertEquals(
            listOf(firstEntityURI, secondEntityURI),
            batchOperationResult.getSuccessfulEntitiesIds()
        )
        assertTrue(batchOperationResult.errors.isEmpty())

        coVerify { entityAttributeService.deleteTemporalAttributesOfEntity(firstEntityURI) }
        coVerify { entityAttributeService.deleteTemporalAttributesOfEntity(secondEntityURI) }
        coVerify {
            entityService.appendAttributes(eq(firstEntityURI), any(), false, sub)
        }
        coVerify {
            entityService.appendAttributes(eq(secondEntityURI), any(), false, sub)
        }
        coVerify(exactly = 2) {
            entityEventService.publishEntityReplaceEvent(any(), any(), any())
        }
    }

    @Test
    fun `batch delete should return the list of deleted entity ids when deletion is successful`() = runTest {
        coEvery { entityService.deleteEntity(any()) } returns mockkClass(EntityPayload::class).right()
        coEvery { authorizationService.removeRightsOnEntity(any()) } returns Unit.right()
        coEvery { entityEventService.publishEntityDeleteEvent(any(), any()) } returns Job()

        val batchOperationResult = entityOperationService.delete(
            setOf(
                mockkClass(EntityPayload::class) { every { entityId } returns firstEntityURI },
                mockkClass(EntityPayload::class) { every { entityId } returns secondEntityURI },
            ),
            sub
        )

        assertEquals(
            listOf(firstEntityURI, secondEntityURI),
            batchOperationResult.getSuccessfulEntitiesIds()
        )
        assertEquals(emptyList<BatchEntityError>(), batchOperationResult.errors)

        coVerify {
            entityService.deleteEntity(firstEntityURI)
            entityService.deleteEntity(secondEntityURI)
            authorizationService.removeRightsOnEntity(firstEntityURI)
            authorizationService.removeRightsOnEntity(secondEntityURI)
        }
        coVerify(exactly = 2) {
            entityEventService.publishEntityDeleteEvent(sub, any())
        }
    }

    @Test
    fun `batch delete should return deleted entity ids and in errors when deletion is partially successful`() =
        runTest {
            coEvery {
                entityService.deleteEntity(firstEntityURI)
            } returns mockkClass(EntityPayload::class).right()
            coEvery {
                entityService.deleteEntity(secondEntityURI)
            } returns InternalErrorException("Something went wrong during deletion").left()
            coEvery { authorizationService.removeRightsOnEntity(any()) } returns Unit.right()
            coEvery { entityEventService.publishEntityDeleteEvent(any(), any()) } returns Job()

            val batchOperationResult = entityOperationService.delete(
                setOf(
                    mockkClass(EntityPayload::class) { every { entityId } returns firstEntityURI },
                    mockkClass(EntityPayload::class) { every { entityId } returns secondEntityURI },
                ),
                sub
            )

            assertEquals(
                listOf(BatchEntitySuccess(firstEntityURI)),
                batchOperationResult.success
            )
            assertEquals(
                listOf(
                    BatchEntityError(
                        secondEntityURI,
                        mutableListOf("Something went wrong during deletion")
                    )
                ),
                batchOperationResult.errors
            )
            coVerify(exactly = 1) {
                authorizationService.removeRightsOnEntity(any())
                entityEventService.publishEntityDeleteEvent(any(), any())
            }
        }

    @Test
    fun `batch delete should return error messages when deletion in DB has failed`() = runTest {
        val deleteEntityErrorMessage = "Something went wrong with deletion request"

        coEvery {
            entityService.deleteEntity(any())
        } returns InternalErrorException(deleteEntityErrorMessage).left()

        val batchOperationResult = entityOperationService.delete(
            setOf(
                mockkClass(EntityPayload::class) { every { entityId } returns firstEntityURI },
                mockkClass(EntityPayload::class) { every { entityId } returns secondEntityURI },
            ),
            sub
        )

        assertEquals(emptyList<BatchEntitySuccess>(), batchOperationResult.success)
        assertEquals(
            listOf(
                BatchEntityError(
                    firstEntityURI,
                    mutableListOf(deleteEntityErrorMessage)
                ),
                BatchEntityError(
                    secondEntityURI,
                    mutableListOf(deleteEntityErrorMessage)
                )
            ),
            batchOperationResult.errors
        )

        coVerify { entityService.deleteEntity(firstEntityURI) }
        coVerify { entityService.deleteEntity(secondEntityURI) }
        coVerify { entityEventService.publishEntityDeleteEvent(any(), any()) wasNot Called }
    }

    @Test
    fun `batch merge should ask to merge attributes of entities`() = runTest {
        coEvery {
            entityService.mergeEntity(any(), any(), any(), any())
        } returns EMPTY_UPDATE_RESULT.right()
        coEvery { entityEventService.publishAttributeChangeEvents(any(), any(), any(), any(), any()) } returns Job()

        val batchOperationResult = entityOperationService.merge(
            listOf(
                firstExpandedEntity to firstEntity,
                secondExpandedEntity to secondEntity
            ),
            sub
        )

        assertEquals(
            listOf(firstEntityURI, secondEntityURI),
            batchOperationResult.getSuccessfulEntitiesIds()
        )

        coVerify {
            entityService.mergeEntity(eq(firstEntityURI), any(), null, sub)
            entityService.mergeEntity(eq(secondEntityURI), any(), null, sub)
        }
        coVerify(exactly = 2) {
            entityEventService.publishAttributeChangeEvents(any(), any(), any(), any(), any())
        }
    }
}
