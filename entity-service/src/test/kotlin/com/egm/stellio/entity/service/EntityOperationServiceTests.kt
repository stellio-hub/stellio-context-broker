package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.NotUpdatedDetails
import com.egm.stellio.entity.model.UpdateResult
import com.egm.stellio.entity.repository.Neo4jRepository
import com.egm.stellio.entity.web.BatchEntityError
import com.egm.stellio.entity.web.BatchEntitySuccess
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
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

    @MockkBean
    private lateinit var neo4jRepository: Neo4jRepository

    @Autowired
    private lateinit var entityOperationService: EntityOperationService

    val firstEntityURI = "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
    val secondEntityURI = "urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()
    private lateinit var firstEntity: NgsiLdEntity
    private lateinit var secondEntity: NgsiLdEntity

    @BeforeEach
    fun initNgsiLdEntitiesMocks() {
        firstEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { firstEntity.id } returns firstEntityURI
        secondEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { secondEntity.id } returns secondEntityURI
    }

    @Test
    fun `it should split entities per existence`() {
        every {
            neo4jRepository.filterExistingEntitiesAsIds(listOf(firstEntityURI, secondEntityURI))
        } returns listOf(firstEntityURI)

        val (exist, doNotExist) = entityOperationService.splitEntitiesByExistence(listOf(firstEntity, secondEntity))

        assertEquals(listOf(firstEntity), exist)
        assertEquals(listOf(secondEntity), doNotExist)
    }

    @Test
    fun `it should split entities per existence with ids`() {
        every {
            neo4jRepository.filterExistingEntitiesAsIds(listOf(firstEntityURI, secondEntityURI))
        } returns listOf(firstEntityURI)

        val (exist, doNotExist) =
            entityOperationService.splitEntitiesIdsByExistence(listOf(firstEntityURI, secondEntityURI))

        assertEquals(listOf(firstEntityURI), exist)
        assertEquals(listOf(secondEntityURI), doNotExist)
    }

    @Test
    fun `it should ask to create all provided entities`() {
        every { entityService.createEntity(firstEntity) } returns firstEntity.id
        every { entityService.createEntity(secondEntity) } returns secondEntity.id

        val batchOperationResult = entityOperationService.create(listOf(firstEntity, secondEntity))

        assertEquals(
            arrayListOf(firstEntityURI, secondEntityURI),
            batchOperationResult.getSuccessfulEntitiesIds()
        )
        assertTrue(batchOperationResult.errors.isEmpty())

        verify { entityService.createEntity(firstEntity) }
        verify { entityService.createEntity(secondEntity) }
        confirmVerified(entityService)
    }

    @Test
    fun `it should ask to create entities and transmit back any error`() {
        every { entityService.createEntity(firstEntity) } returns firstEntity.id
        every { entityService.createEntity(secondEntity) } throws BadRequestDataException("Invalid entity")

        val batchOperationResult = entityOperationService.create(listOf(firstEntity, secondEntity))

        assertEquals(arrayListOf(BatchEntitySuccess(firstEntityURI)), batchOperationResult.success)
        assertEquals(
            arrayListOf(
                BatchEntityError(secondEntityURI, arrayListOf("Invalid entity"))
            ),
            batchOperationResult.errors
        )
    }

    @Test
    fun `it should ask to update attributes of entities`() {
        every { entityService.appendEntityTypes(any(), any()) } returns UpdateResult(emptyList(), emptyList())
        every {
            entityService.appendEntityAttributes(eq(firstEntityURI), any(), any())
        } returns UpdateResult(
            emptyList(),
            emptyList()
        )
        every {
            entityService.appendEntityAttributes(eq(secondEntityURI), any(), any())
        } returns UpdateResult(
            emptyList(),
            emptyList()
        )

        val batchOperationResult = entityOperationService.update(listOf(firstEntity, secondEntity))

        assertEquals(
            listOf(firstEntityURI, secondEntityURI),
            batchOperationResult.getSuccessfulEntitiesIds()
        )

        verify(exactly = 2) { entityService.appendEntityTypes(any(), any()) }
        verify { entityService.appendEntityAttributes(eq(firstEntityURI), any(), false) }
        verify { entityService.appendEntityAttributes(eq(secondEntityURI), any(), false) }
        confirmVerified(entityService)
    }

    @Test
    fun `it should count as error an update which raises a BadRequestDataException`() {
        every { entityService.appendEntityTypes(any(), any()) } returns UpdateResult(emptyList(), emptyList())
        every {
            entityService.appendEntityAttributes(eq(firstEntityURI), any(), any())
        } returns UpdateResult(
            emptyList(),
            emptyList()
        )
        every {
            entityService.appendEntityAttributes(eq(secondEntityURI), any(), any())
        } throws BadRequestDataException("error")

        val batchOperationResult =
            entityOperationService.update(listOf(firstEntity, secondEntity))

        assertEquals(
            listOf(BatchEntitySuccess(firstEntityURI, updateResult = UpdateResult(emptyList(), emptyList()))),
            batchOperationResult.success
        )
        assertEquals(
            listOf(BatchEntityError(secondEntityURI, arrayListOf("error"))),
            batchOperationResult.errors
        )
    }

    @Test
    fun `it should count as error not updated attributes in entities`() {
        val updateResult = UpdateResult(
            emptyList(),
            listOf(
                NotUpdatedDetails("attribute#1", "reason"),
                NotUpdatedDetails("attribute#2", "reason")
            )
        )
        every { entityService.appendEntityTypes(any(), any()) } returns UpdateResult(emptyList(), emptyList())
        every {
            entityService.appendEntityAttributes(eq(firstEntityURI), any(), any())
        } returns UpdateResult(
            emptyList(),
            emptyList()
        )
        every { entityService.appendEntityAttributes(eq(secondEntityURI), any(), any()) } returns updateResult

        val batchOperationResult = entityOperationService.update(listOf(firstEntity, secondEntity))

        assertEquals(
            listOf(BatchEntitySuccess(firstEntityURI, UpdateResult(emptyList(), emptyList()))),
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
    fun `it should ask to replace entities`() {
        every { neo4jRepository.deleteEntityAttributes(firstEntityURI) } returns mockk()
        every { entityService.appendEntityTypes(any(), any()) } returns UpdateResult(emptyList(), emptyList())
        every {
            entityService.appendEntityAttributes(eq(firstEntityURI), any(), any())
        } returns UpdateResult(
            emptyList(),
            emptyList()
        )
        every { neo4jRepository.deleteEntityAttributes(secondEntityURI) } returns mockk()
        every {
            entityService.appendEntityAttributes(eq(secondEntityURI), any(), any())
        } returns UpdateResult(
            emptyList(),
            emptyList()
        )

        val batchOperationResult = entityOperationService.replace(listOf(firstEntity, secondEntity))

        assertEquals(
            listOf(firstEntityURI, secondEntityURI),
            batchOperationResult.getSuccessfulEntitiesIds()
        )
        assertTrue(batchOperationResult.errors.isEmpty())

        verify { neo4jRepository.deleteEntityAttributes(firstEntityURI) }
        verify { neo4jRepository.deleteEntityAttributes(secondEntityURI) }
        confirmVerified(neo4jRepository)

        verify(exactly = 2) { entityService.appendEntityTypes(any(), any()) }
        verify { entityService.appendEntityAttributes(eq(firstEntityURI), any(), false) }
        verify { entityService.appendEntityAttributes(eq(secondEntityURI), any(), false) }
        confirmVerified(entityService)
    }

    @Test
    fun `it should count as error an replace which raises a BadRequestDataException`() {
        every { neo4jRepository.deleteEntityAttributes(firstEntityURI) } returns mockk()
        every { entityService.appendEntityTypes(any(), any()) } returns UpdateResult(emptyList(), emptyList())
        every {
            entityService.appendEntityAttributes(eq(firstEntityURI), any(), any())
        } returns UpdateResult(
            emptyList(),
            emptyList()
        )
        every { neo4jRepository.deleteEntityAttributes(secondEntityURI) } returns mockk()
        every {
            entityService.appendEntityAttributes(eq(secondEntityURI), any(), any())
        } throws BadRequestDataException("error")

        val batchOperationResult = entityOperationService.replace(listOf(firstEntity, secondEntity))

        assertEquals(listOf(BatchEntitySuccess(firstEntityURI)), batchOperationResult.success)
        assertEquals(
            listOf(BatchEntityError(secondEntityURI, arrayListOf("error"))),
            batchOperationResult.errors
        )
    }

    @Test
    fun `it should count as error not replaced entities in entities`() {
        every { neo4jRepository.deleteEntityAttributes(firstEntityURI) } returns mockk()
        every { entityService.appendEntityTypes(any(), any()) } returns UpdateResult(emptyList(), emptyList())
        every {
            entityService.appendEntityAttributes(eq(firstEntityURI), any(), any())
        } returns UpdateResult(
            emptyList(),
            emptyList()
        )
        every { neo4jRepository.deleteEntityAttributes(secondEntityURI) } returns mockk()
        every {
            entityService.appendEntityAttributes(eq(secondEntityURI), any(), any())
        } returns UpdateResult(
            emptyList(),
            listOf(
                NotUpdatedDetails("attribute#1", "reason"),
                NotUpdatedDetails("attribute#2", "reason")
            )
        )

        val batchOperationResult = entityOperationService.replace(listOf(firstEntity, secondEntity))

        assertEquals(listOf(BatchEntitySuccess(firstEntityURI)), batchOperationResult.success)
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
    fun `it should return the list of deleted entity ids when deletion is successful`() {
        every { entityService.deleteEntity(any()) } returns Pair(1, 1)

        val batchOperationResult = entityOperationService.delete(setOf(firstEntityURI, secondEntityURI))

        assertEquals(
            listOf(firstEntityURI, secondEntityURI),
            batchOperationResult.getSuccessfulEntitiesIds()
        )
        assertEquals(emptyList<BatchEntityError>(), batchOperationResult.errors)

        verify { entityService.deleteEntity(firstEntityURI) }
        verify { entityService.deleteEntity(secondEntityURI) }
        confirmVerified(entityService)
    }

    @Test
    fun `it should return the list of deleted entity ids and in errors when deletion is partially successful`() {
        every { entityService.deleteEntity(firstEntityURI) } returns Pair(1, 1)
        every {
            entityService.deleteEntity(secondEntityURI)
        } throws RuntimeException("Something went wrong during deletion")

        val batchOperationResult = entityOperationService.delete(setOf(firstEntityURI, secondEntityURI))

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
    }

    @Test
    fun `it should return error messages when deletion in DB has failed`() {
        val deleteEntityErrorMessage = "Something went wrong with deletion request"

        every {
            entityService.deleteEntity(any())
        } throws RuntimeException(deleteEntityErrorMessage)

        val batchOperationResult = entityOperationService.delete(setOf(firstEntityURI, secondEntityURI))

        assertEquals(emptyList<String>(), batchOperationResult.success)
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

        verify { entityService.deleteEntity(firstEntityURI) }
        verify { entityService.deleteEntity(secondEntityURI) }
        confirmVerified(entityService)
    }
}
