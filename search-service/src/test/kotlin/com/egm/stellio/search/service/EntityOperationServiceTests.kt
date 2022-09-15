package com.egm.stellio.search.service

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.model.NotUpdatedDetails
import com.egm.stellio.search.model.UpdateResult
import com.egm.stellio.search.web.BatchEntityError
import com.egm.stellio.search.web.BatchEntitySuccess
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.InternalErrorException
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockkClass
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@OptIn(ExperimentalCoroutinesApi::class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [EntityOperationService::class])
@ActiveProfiles("test")
class EntityOperationServiceTests {

    @MockkBean
    private lateinit var entityPayloadService: EntityPayloadService

    @MockkBean
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @Autowired
    private lateinit var entityOperationService: EntityOperationService

    val firstEntityURI = "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
    val secondEntityURI = "urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()
    private lateinit var firstJsonLdEntity: JsonLdEntity
    private lateinit var firstEntity: NgsiLdEntity
    private lateinit var secondJsonLdEntity: JsonLdEntity
    private lateinit var secondEntity: NgsiLdEntity

    val sub: Sub = "60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"

    @BeforeEach
    fun initNgsiLdEntitiesMocks() {
        firstJsonLdEntity = mockkClass(JsonLdEntity::class, relaxed = true) {
            every { id } returns firstEntityURI.toString()
            every { properties } returns emptyMap()
        }
        firstEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { firstEntity.id } returns firstEntityURI
        secondJsonLdEntity = mockkClass(JsonLdEntity::class, relaxed = true) {
            every { id } returns secondEntityURI.toString()
            every { properties } returns emptyMap()
        }
        secondEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { secondEntity.id } returns secondEntityURI
    }

    @Test
    fun `it should split entities per existence`() = runTest {
        coEvery {
            entityPayloadService.filterExistingEntitiesAsIds(listOf(firstEntityURI, secondEntityURI))
        } returns listOf(firstEntityURI)

        val (exist, doNotExist) = entityOperationService.splitEntitiesByExistence(listOf(firstEntity, secondEntity))

        assertEquals(listOf(firstEntity), exist)
        assertEquals(listOf(secondEntity), doNotExist)
    }

    @Test
    fun `it should split entities per existence with ids`() = runTest {
        coEvery {
            entityPayloadService.filterExistingEntitiesAsIds(listOf(firstEntityURI, secondEntityURI))
        } returns listOf(firstEntityURI)

        val (exist, doNotExist) =
            entityOperationService.splitEntitiesIdsByExistence(listOf(firstEntityURI, secondEntityURI))

        assertEquals(listOf(firstEntityURI), exist)
        assertEquals(listOf(secondEntityURI), doNotExist)
    }

    @Test
    fun `it should ask to create all provided entities`() = runTest {
        coEvery {
            temporalEntityAttributeService.createEntityTemporalReferences(any(), any(), any(), any())
        } returns Unit.right()

        val batchOperationResult = entityOperationService.create(
            listOf(firstEntity, secondEntity),
            listOf(firstJsonLdEntity, secondJsonLdEntity),
            sub
        )

        assertEquals(
            arrayListOf(firstEntityURI, secondEntityURI),
            batchOperationResult.getSuccessfulEntitiesIds()
        )
        assertTrue(batchOperationResult.errors.isEmpty())

        coVerify {
            temporalEntityAttributeService.createEntityTemporalReferences(firstEntity, firstJsonLdEntity, any(), sub)
        }
        coVerify {
            temporalEntityAttributeService.createEntityTemporalReferences(secondEntity, secondJsonLdEntity, any(), sub)
        }
    }

    @Test
    fun `it should ask to create entities and transmit back any error`() = runTest {
        coEvery {
            temporalEntityAttributeService.createEntityTemporalReferences(firstEntity, any(), any(), any())
        } returns Unit.right()
        coEvery {
            temporalEntityAttributeService.createEntityTemporalReferences(secondEntity, any(), any(), any())
        } returns BadRequestDataException("Invalid entity").left()

        val batchOperationResult = entityOperationService.create(
            listOf(firstEntity, secondEntity),
            listOf(firstJsonLdEntity, secondJsonLdEntity),
            sub
        )

        assertEquals(arrayListOf(BatchEntitySuccess(firstEntityURI)), batchOperationResult.success)
        assertEquals(
            arrayListOf(
                BatchEntityError(secondEntityURI, arrayListOf("Invalid entity"))
            ),
            batchOperationResult.errors
        )
    }

    @Test
    fun `it should ask to update attributes of entities`() = runTest {
        coEvery {
            entityPayloadService.updateTypes(any(), any(), any())
        } returns UpdateResult(emptyList(), emptyList()).right()
        coEvery {
            temporalEntityAttributeService.appendEntityAttributes(any(), any(), any(), any(), any())
        } returns UpdateResult(emptyList(), emptyList()).right()

        val batchOperationResult = entityOperationService.update(
            listOf(Pair(firstEntity, firstJsonLdEntity), Pair(secondEntity, secondJsonLdEntity)),
            false,
            sub
        )

        assertEquals(
            listOf(firstEntityURI, secondEntityURI),
            batchOperationResult.getSuccessfulEntitiesIds()
        )

        coVerify(exactly = 2) { entityPayloadService.updateTypes(any(), any(), false) }
        coVerify {
            temporalEntityAttributeService.appendEntityAttributes(eq(firstEntityURI), any(), any(), false, sub)
        }
        coVerify {
            temporalEntityAttributeService.appendEntityAttributes(eq(secondEntityURI), any(), any(), false, sub)
        }
    }

    @Test
    fun `it should count as error an update which raises a BadRequestDataException`() = runTest {
        coEvery {
            entityPayloadService.updateTypes(any(), any(), any())
        } returns UpdateResult(emptyList(), emptyList()).right()
        coEvery {
            temporalEntityAttributeService.appendEntityAttributes(firstEntityURI, any(), any(), any(), any())
        } returns UpdateResult(emptyList(), emptyList()).right()
        coEvery {
            temporalEntityAttributeService.appendEntityAttributes(secondEntityURI, any(), any(), any(), any())
        } returns BadRequestDataException("error").left()

        val batchOperationResult =
            entityOperationService.update(
                listOf(Pair(firstEntity, firstJsonLdEntity), Pair(secondEntity, secondJsonLdEntity)),
                false,
                sub
            )

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
    fun `it should count as error not updated attributes in entities`() = runTest {
        val updateResult = UpdateResult(
            emptyList(),
            listOf(
                NotUpdatedDetails("attribute#1", "reason"),
                NotUpdatedDetails("attribute#2", "reason")
            )
        )
        coEvery {
            entityPayloadService.updateTypes(any(), any(), any())
        } returns UpdateResult(emptyList(), emptyList()).right()
        coEvery {
            temporalEntityAttributeService.appendEntityAttributes(firstEntityURI, any(), any(), any(), any())
        } returns UpdateResult(emptyList(), emptyList()).right()
        coEvery {
            temporalEntityAttributeService.appendEntityAttributes(secondEntityURI, any(), any(), any(), any())
        } returns updateResult.right()

        val batchOperationResult = entityOperationService.update(
            listOf(Pair(firstEntity, firstJsonLdEntity), Pair(secondEntity, secondJsonLdEntity)),
            false,
            sub
        )

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
    fun `it should ask to replace entities`() = runTest {
        coEvery {
            temporalEntityAttributeService.deleteTemporalAttributesOfEntity(any())
        } returns Unit.right()
        coEvery {
            entityPayloadService.updateTypes(any(), any(), any())
        } returns UpdateResult(emptyList(), emptyList()).right()
        coEvery {
            temporalEntityAttributeService.appendEntityAttributes(any(), any(), any(), any(), any())
        } returns UpdateResult(emptyList(), emptyList()).right()

        val batchOperationResult = entityOperationService.replace(
            listOf(Pair(firstEntity, firstJsonLdEntity), Pair(secondEntity, secondJsonLdEntity)),
            sub
        )

        assertEquals(
            listOf(firstEntityURI, secondEntityURI),
            batchOperationResult.getSuccessfulEntitiesIds()
        )
        assertTrue(batchOperationResult.errors.isEmpty())

        coVerify { temporalEntityAttributeService.deleteTemporalAttributesOfEntity(firstEntityURI) }
        coVerify { temporalEntityAttributeService.deleteTemporalAttributesOfEntity(secondEntityURI) }

        coVerify(exactly = 2) { entityPayloadService.updateTypes(any(), any(), false) }
        coVerify {
            temporalEntityAttributeService.appendEntityAttributes(eq(firstEntityURI), any(), any(), false, sub)
        }
        coVerify {
            temporalEntityAttributeService.appendEntityAttributes(eq(secondEntityURI), any(), any(), false, sub)
        }
    }

    @Test
    fun `it should count as error an replace which raises a BadRequestDataException`() = runTest {
        coEvery {
            temporalEntityAttributeService.deleteTemporalAttributesOfEntity(any())
        } returns Unit.right()
        coEvery {
            entityPayloadService.updateTypes(any(), any(), any())
        } returns UpdateResult(emptyList(), emptyList()).right()
        coEvery {
            temporalEntityAttributeService.appendEntityAttributes(firstEntityURI, any(), any(), any(), any())
        } returns UpdateResult(emptyList(), emptyList()).right()
        coEvery {
            temporalEntityAttributeService.appendEntityAttributes(secondEntityURI, any(), any(), any(), any())
        } returns BadRequestDataException("error").left()

        val batchOperationResult = entityOperationService.replace(
            listOf(Pair(firstEntity, firstJsonLdEntity), Pair(secondEntity, secondJsonLdEntity)),
            sub
        )

        assertEquals(listOf(BatchEntitySuccess(firstEntityURI)), batchOperationResult.success)
        assertEquals(
            listOf(BatchEntityError(secondEntityURI, arrayListOf("error"))),
            batchOperationResult.errors
        )
    }

    @Test
    fun `it should count as error not replaced entities in entities`() = runTest {
        coEvery {
            temporalEntityAttributeService.deleteTemporalAttributesOfEntity(any())
        } returns Unit.right()
        coEvery {
            entityPayloadService.updateTypes(any(), any(), any())
        } returns UpdateResult(emptyList(), emptyList()).right()
        coEvery {
            temporalEntityAttributeService.appendEntityAttributes(firstEntityURI, any(), any(), any(), any())
        } returns UpdateResult(emptyList(), emptyList()).right()
        coEvery {
            temporalEntityAttributeService.appendEntityAttributes(secondEntityURI, any(), any(), any(), any())
        } returns UpdateResult(
            emptyList(),
            listOf(
                NotUpdatedDetails("attribute#1", "reason"),
                NotUpdatedDetails("attribute#2", "reason")
            )
        ).right()

        val batchOperationResult = entityOperationService.replace(
            listOf(Pair(firstEntity, firstJsonLdEntity), Pair(secondEntity, secondJsonLdEntity)),
            sub
        )

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
    fun `it should return the list of deleted entity ids when deletion is successful`() = runTest {
        coEvery { temporalEntityAttributeService.deleteTemporalEntityReferences(any()) } returns Unit.right()

        val batchOperationResult = entityOperationService.delete(setOf(firstEntityURI, secondEntityURI))

        assertEquals(
            listOf(firstEntityURI, secondEntityURI),
            batchOperationResult.getSuccessfulEntitiesIds()
        )
        assertEquals(emptyList<BatchEntityError>(), batchOperationResult.errors)

        coVerify { temporalEntityAttributeService.deleteTemporalEntityReferences(firstEntityURI) }
        coVerify { temporalEntityAttributeService.deleteTemporalEntityReferences(secondEntityURI) }
    }

    @Test
    fun `it should return the list of deleted entity ids and in errors when deletion is partially successful`() =
        runTest {
            coEvery {
                temporalEntityAttributeService.deleteTemporalEntityReferences(firstEntityURI)
            } returns Unit.right()
            coEvery {
                temporalEntityAttributeService.deleteTemporalEntityReferences(secondEntityURI)
            } returns InternalErrorException("Something went wrong during deletion").left()

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
    fun `it should return error messages when deletion in DB has failed`() = runTest {
        val deleteEntityErrorMessage = "Something went wrong with deletion request"

        coEvery {
            temporalEntityAttributeService.deleteTemporalEntityReferences(any())
        } returns InternalErrorException(deleteEntityErrorMessage).left()

        val batchOperationResult = entityOperationService.delete(setOf(firstEntityURI, secondEntityURI))

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

        coVerify { temporalEntityAttributeService.deleteTemporalEntityReferences(firstEntityURI) }
        coVerify { temporalEntityAttributeService.deleteTemporalEntityReferences(secondEntityURI) }
    }
}