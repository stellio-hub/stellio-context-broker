package com.egm.stellio.search.entity.service

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.entity.model.EMPTY_UPDATE_RESULT
import com.egm.stellio.search.entity.model.NotUpdatedDetails
import com.egm.stellio.search.entity.model.UpdateResult
import com.egm.stellio.search.entity.web.BatchEntityError
import com.egm.stellio.search.entity.web.BatchEntitySuccess
import com.egm.stellio.search.entity.web.BatchOperationResult
import com.egm.stellio.search.entity.web.JsonLdNgsiLdEntity
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.InternalErrorException
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.util.ENTITIY_CREATION_FORBIDDEN_MESSAGE
import com.egm.stellio.shared.util.ENTITY_ADMIN_FORBIDDEN_MESSAGE
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import com.ninjasquad.springmockk.SpykBean
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockkClass
import io.mockk.slot
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [EntityOperationService::class])
@ActiveProfiles("test")
class EntityOperationServiceTests {

    @MockkBean
    private lateinit var entityService: EntityService

    @MockkBean
    private lateinit var entityQueryService: EntityQueryService

    @SpykBean
    private lateinit var entityOperationService: EntityOperationService

    val firstEntityURI = "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
    val secondEntityURI = "urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()
    private lateinit var firstExpandedEntity: ExpandedEntity
    private lateinit var firstEntity: NgsiLdEntity
    private lateinit var secondExpandedEntity: ExpandedEntity
    private lateinit var secondEntity: NgsiLdEntity

    @BeforeAll
    fun initNgsiLdEntitiesMocks() {
        firstExpandedEntity = mockkClass(ExpandedEntity::class, relaxed = true)
        firstEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { firstEntity.id } returns firstEntityURI
        secondExpandedEntity = mockkClass(ExpandedEntity::class, relaxed = true)
        secondEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { secondEntity.id } returns secondEntityURI
    }

    @Test
    fun `splitEntitiesByExistence should split entities per existence`() = runTest {
        coEvery {
            entityQueryService.filterExistingEntitiesAsIds(listOf(firstEntityURI, secondEntityURI))
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
            entityQueryService.filterExistingEntitiesAsIds(listOf(firstEntityURI, secondEntityURI))
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
        val error = BadRequestDataException("error")

        coEvery {
            entityService.appendAttributes(firstEntityURI, any(), any())
        } returns EMPTY_UPDATE_RESULT.right()
        coEvery {
            entityService.appendAttributes(secondEntityURI, any(), any())
        } returns error.left()

        val batchOperationResult =
            entityOperationService.processEntities(
                listOf(
                    firstExpandedEntity to firstEntity,
                    secondExpandedEntity to secondEntity
                ),
                processor = entityOperationService::updateEntity
            )

        assertEquals(
            listOf(BatchEntitySuccess(firstEntityURI, updateResult = EMPTY_UPDATE_RESULT)),
            batchOperationResult.success
        )
        assertEquals(
            listOf(BatchEntityError(secondEntityURI, error.toProblemDetail())),
            batchOperationResult.errors
        )
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
            entityService.appendAttributes(firstEntityURI, any(), any())
        } returns EMPTY_UPDATE_RESULT.right()
        coEvery {
            entityService.appendAttributes(secondEntityURI, any(), any())
        } returns updateResult.right()

        val batchOperationResult = entityOperationService.processEntities(
            listOf(
                firstExpandedEntity to firstEntity,
                secondExpandedEntity to secondEntity
            ),
            processor = entityOperationService::updateEntity
        )

        assertEquals(
            listOf(BatchEntitySuccess(firstEntityURI, EMPTY_UPDATE_RESULT)),
            batchOperationResult.success
        )
        assertEquals(
            listOf(
                BatchEntityError(
                    secondEntityURI,
                    BadRequestDataException("attribute#1 : reason, attribute#2 : reason").toProblemDetail()
                )
            ),
            batchOperationResult.errors
        )
    }

    @Test
    fun `batch create should ask to create all provided entities`() = runTest {
        coEvery { entityService.createEntity(any<NgsiLdEntity>(), any()) } returns Unit.right()

        val batchOperationResult = entityOperationService.create(
            listOf(
                Pair(firstExpandedEntity, firstEntity),
                Pair(secondExpandedEntity, secondEntity)
            )
        )

        assertEquals(
            arrayListOf(firstEntityURI, secondEntityURI),
            batchOperationResult.getSuccessfulEntitiesIds()
        )
        assertTrue(batchOperationResult.errors.isEmpty())

        coVerify {
            entityService.createEntity(firstEntity, firstExpandedEntity)
        }
        coVerify {
            entityService.createEntity(secondEntity, secondExpandedEntity)
        }
    }

    @Test
    fun `batch create should ask to create entities and transmit back any error`() = runTest {
        val badRequestException = BadRequestDataException("Invalid entity")
        coEvery { entityService.createEntity(firstEntity, any()) } returns Unit.right()
        coEvery {
            entityService.createEntity(secondEntity, any())
        } returns badRequestException.left()

        val batchOperationResult = entityOperationService.create(
            listOf(
                Pair(firstExpandedEntity, firstEntity),
                Pair(secondExpandedEntity, secondEntity)
            )
        )

        assertEquals(arrayListOf(BatchEntitySuccess(firstEntityURI)), batchOperationResult.success)
        assertEquals(
            arrayListOf(
                BatchEntityError(secondEntityURI, badRequestException.toProblemDetail())
            ),
            batchOperationResult.errors
        )
        coVerify(exactly = 1) {
            entityService.createEntity(secondEntity, any())
        }
    }

    @Test
    fun `batch update should ask to update attributes of entities`() = runTest {
        coEvery {
            entityService.appendAttributes(any(), any(), any())
        } returns EMPTY_UPDATE_RESULT.right()

        val batchOperationResult = entityOperationService.update(
            listOf(
                Pair(firstExpandedEntity, firstEntity),
                Pair(secondExpandedEntity, secondEntity)
            )
        )

        assertEquals(
            listOf(firstEntityURI, secondEntityURI),
            batchOperationResult.getSuccessfulEntitiesIds()
        )

        coVerify {
            entityService.appendAttributes(eq(firstEntityURI), any(), false)
        }
        coVerify {
            entityService.appendAttributes(eq(secondEntityURI), any(), false)
        }
    }

    @Test
    fun `batch replace should ask to replace entities`() = runTest {
        coEvery {
            entityService.replaceEntity(any(), any(), any())
        } returns EMPTY_UPDATE_RESULT.right()

        val batchOperationResult = entityOperationService.replace(
            listOf(
                Pair(firstExpandedEntity, firstEntity),
                Pair(secondExpandedEntity, secondEntity)
            )
        )

        assertEquals(
            listOf(firstEntityURI, secondEntityURI),
            batchOperationResult.getSuccessfulEntitiesIds()
        )
        assertTrue(batchOperationResult.errors.isEmpty())

        coVerify {
            entityService.replaceEntity(firstEntityURI, any(), any())
            entityService.replaceEntity(secondEntityURI, any(), any())
        }
    }

    @Test
    fun `batch delete should return the list of deleted entity ids when deletion is successful`() = runTest {
        coEvery { entityService.deleteEntity(any()) } returns Unit.right()

        val batchOperationResult = entityOperationService.delete(
            listOf(
                firstEntityURI,
                secondEntityURI,
            )
        )

        assertEquals(
            listOf(firstEntityURI, secondEntityURI),
            batchOperationResult.getSuccessfulEntitiesIds()
        )
        assertEquals(emptyList<BatchEntityError>(), batchOperationResult.errors)

        coVerify {
            entityService.deleteEntity(firstEntityURI)
            entityService.deleteEntity(secondEntityURI)
        }
    }

    @Test
    fun `batch delete should return deleted entity ids and in errors when deletion is partially successful`() =
        runTest {
            val internalError = InternalErrorException("Something went wrong during deletion")
            coEvery { entityService.deleteEntity(firstEntityURI) } returns Unit.right()
            coEvery {
                entityService.deleteEntity(secondEntityURI)
            } returns internalError.left()

            val batchOperationResult = entityOperationService.delete(
                listOf(
                    firstEntityURI,
                    secondEntityURI,
                )
            )

            assertEquals(
                listOf(BatchEntitySuccess(firstEntityURI)),
                batchOperationResult.success
            )
            assertEquals(
                listOf(
                    BatchEntityError(
                        secondEntityURI,
                        internalError.toProblemDetail()
                    )
                ),
                batchOperationResult.errors
            )
        }

    @Test
    fun `batch delete should return error messages when deletion in DB has failed`() = runTest {
        val deleteEntityError = InternalErrorException("Something went wrong with deletion request")

        coEvery {
            entityService.deleteEntity(any())
        } returns deleteEntityError.left()

        val batchOperationResult = entityOperationService.delete(
            listOf(
                firstEntityURI,
                secondEntityURI,
            )
        )

        assertEquals(emptyList<BatchEntitySuccess>(), batchOperationResult.success)
        assertEquals(
            listOf(
                BatchEntityError(
                    firstEntityURI,
                    deleteEntityError.toProblemDetail()
                ),
                BatchEntityError(
                    secondEntityURI,
                    deleteEntityError.toProblemDetail()
                )
            ),
            batchOperationResult.errors
        )

        coVerify { entityService.deleteEntity(firstEntityURI) }
        coVerify { entityService.deleteEntity(secondEntityURI) }
    }

    @Test
    fun `batch merge should ask to merge attributes of entities`() = runTest {
        coEvery {
            entityService.mergeEntity(any(), any(), any())
        } returns EMPTY_UPDATE_RESULT.right()

        val batchOperationResult = entityOperationService.merge(
            listOf(
                firstExpandedEntity to firstEntity,
                secondExpandedEntity to secondEntity
            )
        )

        assertEquals(
            listOf(firstEntityURI, secondEntityURI),
            batchOperationResult.getSuccessfulEntitiesIds()
        )

        coVerify {
            entityService.mergeEntity(eq(firstEntityURI), any(), null)
            entityService.mergeEntity(eq(secondEntityURI), any(), null)
        }
    }

    fun upsertUpdateSetup() {
        val capturedExpandedEntities = slot<List<JsonLdNgsiLdEntity>>()

        coEvery { entityOperationService.splitEntitiesByUniqueness(capture(capturedExpandedEntities)) } answers {
            capturedExpandedEntities.captured to emptyList()
        }

        coEvery { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(
                firstExpandedEntity to firstEntity,
                secondExpandedEntity to secondEntity
            ),
            emptyList()
        )
    }

    @Test
    fun `upsert batch entity without option should replace existing entities`() = runTest {
        upsertUpdateSetup()

        coEvery { entityOperationService.replace(any()) } returns BatchOperationResult(
            mutableListOf(BatchEntitySuccess(firstEntity.id), BatchEntitySuccess(secondEntity.id)),
            arrayListOf()
        )

        val (batchOperationResult, createdIds) = entityOperationService.upsert(
            listOf(
                firstExpandedEntity to firstEntity,
                secondExpandedEntity to secondEntity
            ),
            disallowOverwrite = false,
            updateMode = false
        )

        assertEquals(0, createdIds.size)
        assertEquals(2, batchOperationResult.success.size)
        assertEquals(0, batchOperationResult.errors.size)

        coVerify { entityOperationService.replace(any()) }
        coVerify(exactly = 0) {
            entityOperationService.create(any())
            entityOperationService.update(any(), any())
        }
    }

    @Test
    fun `upsert batch entity with update should update existing entities`() = runTest {
        upsertUpdateSetup()

        coEvery { entityOperationService.update(any(), any()) } returns BatchOperationResult(
            mutableListOf(BatchEntitySuccess(firstEntity.id), BatchEntitySuccess(secondEntity.id)),
            arrayListOf()
        )

        val (batchOperationResult, createdIds) = entityOperationService.upsert(
            listOf(
                firstExpandedEntity to firstEntity,
                secondExpandedEntity to secondEntity
            ),
            disallowOverwrite = false,
            updateMode = true
        )

        assertEquals(0, createdIds.size)
        assertEquals(2, batchOperationResult.success.size)
        assertEquals(0, batchOperationResult.errors.size)

        coVerify { entityOperationService.update(any()) }
        coVerify(exactly = 0) {
            entityOperationService.create(any())
            entityOperationService.replace(any())
        }
    }

    @Test
    fun `upsert batch entity with non existing entities should create them`() = runTest {
        val capturedExpandedEntities = slot<List<JsonLdNgsiLdEntity>>()

        coEvery { entityOperationService.splitEntitiesByUniqueness(capture(capturedExpandedEntities)) } answers {
            capturedExpandedEntities.captured to emptyList()
        }

        coEvery { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            emptyList(),
            listOf(
                firstExpandedEntity to firstEntity,
                secondExpandedEntity to secondEntity
            )
        )

        coEvery { entityOperationService.create(any()) } returns BatchOperationResult(
            mutableListOf(BatchEntitySuccess(firstEntity.id), BatchEntitySuccess(secondEntity.id)),
            arrayListOf()
        )

        val (batchOperationResult, createdIds) = entityOperationService.upsert(
            listOf(
                firstExpandedEntity to firstEntity,
                secondExpandedEntity to secondEntity
            ),
            disallowOverwrite = false,
            updateMode = true
        )

        assertEquals(2, createdIds.size)
        assertEquals(2, batchOperationResult.success.size)
        assertEquals(0, batchOperationResult.errors.size)

        coVerify { entityOperationService.create(any()) }
        coVerify(exactly = 0) {
            entityOperationService.update(any(), any())
            entityOperationService.replace(any())
        }
    }

    @Test
    fun `upsert batch entity should return errors`() = runTest {
        val capturedExpandedEntities = slot<List<JsonLdNgsiLdEntity>>()

        coEvery { entityOperationService.splitEntitiesByUniqueness(capture(capturedExpandedEntities)) } answers {
            capturedExpandedEntities.captured to emptyList()
        }

        coEvery { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(
                firstExpandedEntity to firstEntity,
            ),
            listOf(
                secondExpandedEntity to secondEntity
            )
        )

        coEvery { entityOperationService.create(any()) } returns BatchOperationResult(
            emptyList<BatchEntitySuccess>().toMutableList(),
            arrayListOf(
                BatchEntityError(
                    firstEntity.id,
                    AccessDeniedException(ENTITIY_CREATION_FORBIDDEN_MESSAGE).toProblemDetail()
                )
            )
        )
        coEvery { entityOperationService.replace(any()) } returns BatchOperationResult(
            emptyList<BatchEntitySuccess>().toMutableList(),
            arrayListOf(
                BatchEntityError(
                    secondEntity.id,
                    AccessDeniedException(ENTITY_ADMIN_FORBIDDEN_MESSAGE).toProblemDetail()
                )
            )
        )

        val (batchOperationResult, createdIds) = entityOperationService.upsert(
            listOf(
                firstExpandedEntity to firstEntity,
                secondExpandedEntity to secondEntity
            ),
            disallowOverwrite = false,
            updateMode = false
        )

        assertEquals(0, createdIds.size)
        assertEquals(0, batchOperationResult.success.size)
        assertEquals(2, batchOperationResult.errors.size)

        coVerify { entityOperationService.create(any()) }
        coVerify { entityOperationService.replace(any()) }
    }
}
