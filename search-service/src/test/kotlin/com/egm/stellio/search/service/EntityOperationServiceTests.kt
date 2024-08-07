package com.egm.stellio.search.service

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.authorization.AuthorizationService
import com.egm.stellio.search.model.EMPTY_UPDATE_RESULT
import com.egm.stellio.search.model.EntityPayload
import com.egm.stellio.search.model.NotUpdatedDetails
import com.egm.stellio.search.model.UpdateResult
import com.egm.stellio.search.web.BatchEntityError
import com.egm.stellio.search.web.BatchEntitySuccess
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
    private lateinit var entityPayloadService: EntityPayloadService

    @MockkBean(relaxed = true)
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

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
            entityPayloadService.filterExistingEntitiesAsIds(listOf(firstEntityURI, secondEntityURI))
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
            entityPayloadService.filterExistingEntitiesAsIds(listOf(firstEntityURI, secondEntityURI))
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
            entityPayloadService.appendAttributes(firstEntityURI, any(), any(), any())
        } returns EMPTY_UPDATE_RESULT.right()
        coEvery {
            entityPayloadService.appendAttributes(secondEntityURI, any(), any(), any())
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
            entityPayloadService.appendAttributes(firstEntityURI, any(), any(), any())
        } returns EMPTY_UPDATE_RESULT.right()
        coEvery {
            entityPayloadService.appendAttributes(secondEntityURI, any(), any(), any())
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
        coEvery { entityPayloadService.createEntity(any<NgsiLdEntity>(), any(), any()) } returns Unit.right()
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
            entityPayloadService.createEntity(firstEntity, firstExpandedEntity, sub)
        }
        coVerify {
            entityPayloadService.createEntity(secondEntity, secondExpandedEntity, sub)
        }
        coVerify(exactly = 2) {
            entityEventService.publishEntityCreateEvent(any(), any(), any())
        }
    }

    @Test
    fun `batch create should ask to create entities and transmit back any error`() = runTest {
        coEvery { entityPayloadService.createEntity(firstEntity, any(), any()) } returns Unit.right()
        coEvery {
            entityPayloadService.createEntity(secondEntity, any(), any())
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
            entityPayloadService.appendAttributes(any(), any(), any(), any())
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
            entityPayloadService.appendAttributes(eq(firstEntityURI), any(), false, sub)
        }
        coVerify {
            entityPayloadService.appendAttributes(eq(secondEntityURI), any(), false, sub)
        }
        coVerify(exactly = 2) {
            entityEventService.publishAttributeChangeEvents(any(), any(), any(), any(), any())
        }
    }

    @Test
    fun `batch replace should ask to replace entities`() = runTest {
        coEvery {
            temporalEntityAttributeService.deleteTemporalAttributesOfEntity(any())
        } returns Unit.right()
        coEvery {
            entityPayloadService.appendAttributes(any(), any(), any(), any())
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

        coVerify { temporalEntityAttributeService.deleteTemporalAttributesOfEntity(firstEntityURI) }
        coVerify { temporalEntityAttributeService.deleteTemporalAttributesOfEntity(secondEntityURI) }
        coVerify {
            entityPayloadService.appendAttributes(eq(firstEntityURI), any(), false, sub)
        }
        coVerify {
            entityPayloadService.appendAttributes(eq(secondEntityURI), any(), false, sub)
        }
        coVerify(exactly = 2) {
            entityEventService.publishEntityReplaceEvent(any(), any(), any())
        }
    }

    @Test
    fun `batch delete should return the list of deleted entity ids when deletion is successful`() = runTest {
        coEvery { entityPayloadService.deleteEntity(any()) } returns mockkClass(EntityPayload::class).right()
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
            entityPayloadService.deleteEntity(firstEntityURI)
            entityPayloadService.deleteEntity(secondEntityURI)
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
                entityPayloadService.deleteEntity(firstEntityURI)
            } returns mockkClass(EntityPayload::class).right()
            coEvery {
                entityPayloadService.deleteEntity(secondEntityURI)
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
            entityPayloadService.deleteEntity(any())
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

        coVerify { entityPayloadService.deleteEntity(firstEntityURI) }
        coVerify { entityPayloadService.deleteEntity(secondEntityURI) }
        coVerify { entityEventService.publishEntityDeleteEvent(any(), any()) wasNot Called }
    }

    @Test
    fun `batch merge should ask to merge attributes of entities`() = runTest {
        coEvery {
            entityPayloadService.mergeEntity(any(), any(), any(), any())
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
            entityPayloadService.mergeEntity(eq(firstEntityURI), any(), null, sub)
            entityPayloadService.mergeEntity(eq(secondEntityURI), any(), null, sub)
        }
        coVerify(exactly = 2) {
            entityEventService.publishAttributeChangeEvents(any(), any(), any(), any(), any())
        }
    }
}
