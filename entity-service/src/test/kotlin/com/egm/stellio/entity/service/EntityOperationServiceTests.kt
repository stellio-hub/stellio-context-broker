package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.NotUpdatedDetails
import com.egm.stellio.entity.model.UpdateResult
import com.egm.stellio.entity.repository.Neo4jRepository
import com.egm.stellio.entity.web.BatchEntityError
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.util.toListOfUri
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkClass
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
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

    @Test
    fun `it should split entities per existence`() {
        val firstEntity = mockkClass(NgsiLdEntity::class)
        every { firstEntity.id } returns "1".toUri()
        val secondEntity = mockkClass(NgsiLdEntity::class)
        every { secondEntity.id } returns "2".toUri()

        every {
            neo4jRepository.filterExistingEntitiesAsIds(listOf("1", "2").toListOfUri())
        } returns listOf("1").toListOfUri()

        val (exist, doNotExist) = entityOperationService.splitEntitiesByExistence(listOf(firstEntity, secondEntity))

        assertEquals(listOf(firstEntity), exist)
        assertEquals(listOf(secondEntity), doNotExist)
    }

    @Test
    fun `it should split entities per existence with ids`() {
        every {
            neo4jRepository.filterExistingEntitiesAsIds(listOf(firstEntityURI, secondEntityURI))
        } returns listOf("urn:ngsi-ld:Device:HCMR-AQUABOX1").toListOfUri()

        val (exist, doNotExist) =
            entityOperationService.splitEntitiesIdsByExistence(listOf(firstEntityURI, secondEntityURI))

        assertEquals(listOf(firstEntityURI), exist)
        assertEquals(listOf(secondEntityURI), doNotExist)
    }

    @Test
    fun `it should ask to create all provided entities`() {
        val firstEntity = mockkClass(NgsiLdEntity::class)
        every { firstEntity.id } returns firstEntityURI
        val secondEntity = mockkClass(NgsiLdEntity::class)
        every { secondEntity.id } returns secondEntityURI

        every { entityService.createEntity(firstEntity) } returns firstEntity.id
        every { entityService.createEntity(secondEntity) } returns secondEntity.id

        val batchOperationResult = entityOperationService.create(listOf(firstEntity, secondEntity))

        assertEquals(
            arrayListOf(firstEntityURI, secondEntityURI),
            batchOperationResult.success
        )
        assertTrue(batchOperationResult.errors.isEmpty())

        verify { entityService.createEntity(firstEntity) }
        verify { entityService.createEntity(secondEntity) }
        confirmVerified(entityService)
    }

    @Test
    fun `it should ask to create entities and transmit back any error`() {
        val firstEntity = mockkClass(NgsiLdEntity::class)
        every { firstEntity.id } returns firstEntityURI
        val secondEntity = mockkClass(NgsiLdEntity::class)
        every { secondEntity.id } returns secondEntityURI

        every { entityService.createEntity(firstEntity) } returns firstEntity.id
        every { entityService.createEntity(secondEntity) } throws BadRequestDataException("Invalid entity")

        val batchOperationResult = entityOperationService.create(listOf(firstEntity, secondEntity))

        assertEquals(arrayListOf(firstEntityURI), batchOperationResult.success)
        assertEquals(
            arrayListOf(
                BatchEntityError(secondEntityURI, arrayListOf("Invalid entity"))
            ),
            batchOperationResult.errors
        )
    }

    @Test
    fun `it should ask to update attributes of entities`() {
        val firstEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { firstEntity.id } returns firstEntityURI
        val secondEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { secondEntity.id } returns secondEntityURI

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
            listOf("urn:ngsi-ld:Device:HCMR-AQUABOX1", "urn:ngsi-ld:Device:HCMR-AQUABOX2").toListOfUri(),
            batchOperationResult.success
        )

        verify { entityService.appendEntityAttributes(eq(firstEntityURI), any(), false) }
        verify { entityService.appendEntityAttributes(eq(secondEntityURI), any(), false) }
        confirmVerified(entityService)
    }

    @Test
    fun `it should count as error an update which raises a BadRequestDataException`() {
        val firstEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { firstEntity.id } returns firstEntityURI
        val secondEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { secondEntity.id } returns secondEntityURI

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

        assertEquals(listOf(firstEntityURI), batchOperationResult.success)
        assertEquals(
            listOf(BatchEntityError(secondEntityURI, arrayListOf("error"))),
            batchOperationResult.errors
        )
    }

    @Test
    fun `it should count as error not updated attributes in entities`() {
        val firstEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { firstEntity.id } returns "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
        every { firstEntity.getLinkedEntitiesIds() } returns emptyList()
        val secondEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { secondEntity.id } returns "urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()
        every { secondEntity.getLinkedEntitiesIds() } returns emptyList()

        every { neo4jRepository.filterExistingEntitiesAsIds(listOf()) } returns emptyList()
        every {
            entityService.appendEntityAttributes(eq("urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()), any(), any())
        } returns UpdateResult(
            emptyList(),
            emptyList()
        )
        every {
            entityService.appendEntityAttributes(eq("urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()), any(), any())
        } returns UpdateResult(
            emptyList(),
            listOf(
                NotUpdatedDetails("attribute#1", "reason"),
                NotUpdatedDetails("attribute#2", "reason")
            )
        )

        val batchOperationResult =
            entityOperationService.update(listOf(firstEntity, secondEntity))

        assertEquals(listOf("urn:ngsi-ld:Device:HCMR-AQUABOX1").toListOfUri(), batchOperationResult.success)
        assertEquals(
            listOf(
                BatchEntityError(
                    "urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri(),
                    arrayListOf("attribute#1 : reason", "attribute#2 : reason")
                )
            ),
            batchOperationResult.errors
        )
    }

    @Test
    fun `it should replace entities`() {
        val firstEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { firstEntity.id } returns "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
        every { firstEntity.getLinkedEntitiesIds() } returns listOf()
        val secondEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { secondEntity.id } returns "urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()
        every { secondEntity.getLinkedEntitiesIds() } returns listOf()

        every { neo4jRepository.filterExistingEntitiesAsIds(listOf()) } returns listOf()
        every { neo4jRepository.deleteEntityAttributes("urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()) } returns mockk()
        every {
            entityService.appendEntityAttributes(eq("urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()), any(), any())
        } returns UpdateResult(
            emptyList(),
            emptyList()
        )
        every { neo4jRepository.deleteEntityAttributes("urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()) } returns mockk()
        every {
            entityService.appendEntityAttributes(eq("urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()), any(), any())
        } returns UpdateResult(
            emptyList(),
            emptyList()
        )

        val batchOperationResult =
            entityOperationService.replace(listOf(firstEntity, secondEntity))

        assertEquals(
            listOf("urn:ngsi-ld:Device:HCMR-AQUABOX1", "urn:ngsi-ld:Device:HCMR-AQUABOX2").toListOfUri(),
            batchOperationResult.success
        )
        assertTrue(batchOperationResult.errors.isEmpty())
    }

    @Test
    fun `it should count as error entities that couldn't be replaced`() {
        val firstEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { firstEntity.id } returns "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
        every { firstEntity.getLinkedEntitiesIds() } returns listOf()
        val secondEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { secondEntity.id } returns "urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()
        every { secondEntity.getLinkedEntitiesIds() } returns listOf()

        every { neo4jRepository.filterExistingEntitiesAsIds(listOf()) } returns listOf()
        every { neo4jRepository.deleteEntityAttributes("urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()) } returns mockk()
        every {
            entityService.appendEntityAttributes(eq("urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()), any(), any())
        } returns UpdateResult(
            emptyList(),
            emptyList()
        )
        every { neo4jRepository.deleteEntityAttributes("urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()) } returns mockk()
        every {
            entityService.appendEntityAttributes(eq("urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()), any(), any())
        } throws BadRequestDataException("error")

        val batchOperationResult =
            entityOperationService.replace(listOf(firstEntity, secondEntity))

        assertEquals(listOf("urn:ngsi-ld:Device:HCMR-AQUABOX1").toListOfUri(), batchOperationResult.success)
        assertEquals(
            listOf(BatchEntityError("urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri(), arrayListOf("error"))),
            batchOperationResult.errors
        )
    }

    @Test
    fun `it should count as error entities that couldn't be replaced totally`() {
        val firstEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { firstEntity.id } returns "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
        every { firstEntity.getLinkedEntitiesIds() } returns listOf()
        val secondEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { secondEntity.id } returns "urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()
        every { secondEntity.getLinkedEntitiesIds() } returns listOf()

        every { neo4jRepository.filterExistingEntitiesAsIds(listOf()) } returns listOf()
        every { neo4jRepository.deleteEntityAttributes("urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()) } returns mockk()
        every {
            entityService.appendEntityAttributes(eq("urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()), any(), any())
        } returns UpdateResult(
            emptyList(),
            emptyList()
        )
        every { neo4jRepository.deleteEntityAttributes("urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()) } returns mockk()
        every {
            entityService.appendEntityAttributes(eq("urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()), any(), any())
        } returns UpdateResult(
            emptyList(),
            listOf(
                NotUpdatedDetails("attribute#1", "reason"),
                NotUpdatedDetails("attribute#2", "reason")
            )
        )

        val batchOperationResult = entityOperationService.replace(listOf(firstEntity, secondEntity))

        assertEquals(listOf("urn:ngsi-ld:Device:HCMR-AQUABOX1").toListOfUri(), batchOperationResult.success)
        assertEquals(
            listOf(
                BatchEntityError(
                    "urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri(),
                    arrayListOf("attribute#1 : reason, attribute#2 : reason")
                )
            ),
            batchOperationResult.errors
        )
    }

    @Test
    fun `it should return entity ids in BatchOperationResult when their deletion in DB is successful`() {
        val firstEntity = "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
        val secondEntity = "urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()

        every { entityService.deleteEntity(any()) } returns Pair(1, 1)

        val batchOperationResult = entityOperationService.delete(setOf(firstEntity, secondEntity))

        assertEquals(
            listOf("urn:ngsi-ld:Device:HCMR-AQUABOX1", "urn:ngsi-ld:Device:HCMR-AQUABOX2").toListOfUri(),
            batchOperationResult.success
        )
        assertEquals(emptyList<BatchEntityError>(), batchOperationResult.errors)
    }

    @Test
    fun `it should return entity ids in success and in errors when their deletion in DB is partially successful`() {
        val firstEntity = "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
        val secondEntity = "urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()

        every { entityService.deleteEntity(firstEntity) } returns Pair(1, 1)
        every {
            entityService.deleteEntity(secondEntity)
        } throws RuntimeException("Something went wrong during deletion")

        val batchOperationResult = entityOperationService.delete(setOf(firstEntity, secondEntity))

        assertEquals(
            listOf("urn:ngsi-ld:Device:HCMR-AQUABOX1").toListOfUri(),
            batchOperationResult.success
        )
        assertEquals(
            listOf(
                BatchEntityError(
                    "urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri(),
                    mutableListOf("Something went wrong during deletion")
                )
            ),
            batchOperationResult.errors
        )

        verify { entityService.deleteEntity(firstEntity) }
        verify { entityService.deleteEntity(secondEntity) }
        confirmVerified(entityService)
    }

    @Test
    fun `it should return error messages when deletion in DB has failed`() {
        val firstEntityURI = "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
        val secondEntityURI = "urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()
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
