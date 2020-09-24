package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.NotUpdatedDetails
import com.egm.stellio.entity.model.UpdateResult
import com.egm.stellio.entity.repository.EntityRepository
import com.egm.stellio.entity.repository.Neo4jRepository
import com.egm.stellio.entity.util.EntitiesGraphBuilder
import com.egm.stellio.entity.web.BatchEntityError
import com.egm.stellio.entity.web.BatchOperationResult
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.util.toListOfUri
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.mockkClass
import org.jgrapht.graph.DefaultEdge
import org.jgrapht.graph.DirectedPseudograph
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

    @MockkBean
    private lateinit var entityRepository: EntityRepository

    @MockkBean
    private lateinit var entitiesGraphBuilder: EntitiesGraphBuilder

    @Autowired
    private lateinit var entityOperationService: EntityOperationService

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
        val firstEntity = "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
        val secondEntity = "urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()

        every {
            neo4jRepository.filterExistingEntitiesAsIds(
                listOf("urn:ngsi-ld:Device:HCMR-AQUABOX1", "urn:ngsi-ld:Device:HCMR-AQUABOX2").toListOfUri()
            )
        } returns listOf("urn:ngsi-ld:Device:HCMR-AQUABOX1").toListOfUri()

        val (exist, doNotExist) =
            entityOperationService.splitEntitiesIdsByExistence(listOf(firstEntity, secondEntity))

        assertEquals(listOf(firstEntity), exist)
        assertEquals(listOf(secondEntity), doNotExist)
    }

    @Test
    fun `it should create naively isolated entities`() {
        val firstEntity = mockkClass(NgsiLdEntity::class)
        every { firstEntity.id } returns "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
        val secondEntity = mockkClass(NgsiLdEntity::class)
        every { secondEntity.id } returns "urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()

        val acyclicGraph = DirectedPseudograph<NgsiLdEntity, DefaultEdge>(DefaultEdge::class.java)
        acyclicGraph.addVertex(firstEntity)
        acyclicGraph.addVertex(secondEntity)

        every { entitiesGraphBuilder.build(listOf(firstEntity, secondEntity)) } returns Pair(acyclicGraph, listOf())

        every { entityService.createEntity(firstEntity) } returns mockkClass(Entity::class)
        every { entityService.createEntity(secondEntity) } returns mockkClass(Entity::class)

        val batchOperationResult = entityOperationService.create(listOf(firstEntity, secondEntity))

        assertEquals(
            arrayListOf("urn:ngsi-ld:Device:HCMR-AQUABOX1", "urn:ngsi-ld:Device:HCMR-AQUABOX2").toListOfUri(),
            batchOperationResult.success
        )
        assertTrue(batchOperationResult.errors.isEmpty())
    }

    @Test
    fun `it should create naively isolated entities with an error`() {
        val firstEntity = mockkClass(NgsiLdEntity::class)
        every { firstEntity.id } returns "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
        val secondEntity = mockkClass(NgsiLdEntity::class)
        every { secondEntity.id } returns "urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()

        val acyclicGraph = DirectedPseudograph<NgsiLdEntity, DefaultEdge>(DefaultEdge::class.java)
        acyclicGraph.addVertex(firstEntity)
        acyclicGraph.addVertex(secondEntity)

        every { entitiesGraphBuilder.build(listOf(firstEntity, secondEntity)) } returns Pair(acyclicGraph, listOf())

        every { entityService.createEntity(firstEntity) } returns mockkClass(Entity::class)
        every { entityService.createEntity(secondEntity) } throws BadRequestDataException("Invalid entity")

        val batchOperationResult = entityOperationService.create(listOf(firstEntity, secondEntity))

        assertEquals(arrayListOf("urn:ngsi-ld:Device:HCMR-AQUABOX1").toListOfUri(), batchOperationResult.success)
        assertEquals(
            arrayListOf(
                BatchEntityError("urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri(), arrayListOf("Invalid entity"))
            ),
            batchOperationResult.errors
        )
    }

    @Test
    fun `it should create entities with cyclic dependencies`() {
        val firstEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { firstEntity.id } returns "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
        val secondEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { secondEntity.id } returns "urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()

        val cyclicGraph = DirectedPseudograph<NgsiLdEntity, DefaultEdge>(DefaultEdge::class.java)
        cyclicGraph.addVertex(firstEntity)
        cyclicGraph.addVertex(secondEntity)
        cyclicGraph.addEdge(firstEntity, secondEntity)
        cyclicGraph.addEdge(secondEntity, firstEntity)

        every { entitiesGraphBuilder.build(listOf(firstEntity, secondEntity)) } returns Pair(cyclicGraph, listOf())
        every {
            entityService.appendEntityAttributes(eq("urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()), any(), any())
        } returns mockkClass(UpdateResult::class)
        every {
            entityService.appendEntityAttributes(eq("urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()), any(), any())
        } returns mockkClass(UpdateResult::class)
        every { entityService.publishCreationEvent(any()) } just Runs
        every { entityRepository.save<Entity>(any()) } returns mockk()

        val batchOperationResult = entityOperationService.create(listOf(firstEntity, secondEntity))

        assertEquals(
            arrayListOf("urn:ngsi-ld:Device:HCMR-AQUABOX1", "urn:ngsi-ld:Device:HCMR-AQUABOX2").toListOfUri(),
            batchOperationResult.success
        )
        assertTrue(batchOperationResult.errors.isEmpty())
    }

    @Test
    fun `it should not update entities with relationships to invalid entity not found in DB`() {
        val firstEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { firstEntity.id } returns "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
        every { firstEntity.getLinkedEntitiesIds() } returns emptyList()
        val secondEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { secondEntity.id } returns "urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()
        every { secondEntity.getLinkedEntitiesIds() } returns listOf("urn:ngsi-ld:Device:HCMR-AQUABOX3").toListOfUri()

        every { neo4jRepository.filterExistingEntitiesAsIds(listOf()) } returns emptyList()
        every {
            neo4jRepository.filterExistingEntitiesAsIds(listOf("urn:ngsi-ld:Device:HCMR-AQUABOX3").toListOfUri())
        } returns emptyList()
        every {
            entityService.appendEntityAttributes(eq("urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()), any(), any())
        } returns UpdateResult(
            emptyList(),
            emptyList()
        )

        val batchOperationResult =
            entityOperationService.update(listOf(firstEntity, secondEntity), BatchOperationResult())

        assertEquals(listOf("urn:ngsi-ld:Device:HCMR-AQUABOX1").toListOfUri(), batchOperationResult.success)
        assertEquals(
            listOf(
                BatchEntityError(
                    "urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri(),
                    arrayListOf("Target entity urn:ngsi-ld:Device:HCMR-AQUABOX3 does not exist.")
                )
            ),
            batchOperationResult.errors
        )
    }

    @Test
    fun `it should not update entities with relationships to invalid entity given in BatchOperationResult`() {
        val firstEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { firstEntity.id } returns "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
        every { firstEntity.getLinkedEntitiesIds() } returns emptyList()
        val secondEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { secondEntity.id } returns "urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()
        every { secondEntity.getLinkedEntitiesIds() } returns listOf("urn:ngsi-ld:Device:HCMR-AQUABOX3").toListOfUri()

        every { neo4jRepository.filterExistingEntitiesAsIds(listOf()) } returns emptyList()
        every {
            entityService.appendEntityAttributes(eq("urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()), any(), any())
        } returns UpdateResult(
            emptyList(),
            emptyList()
        )

        val batchOperationResult =
            entityOperationService.update(
                listOf(firstEntity, secondEntity),
                BatchOperationResult(
                    errors = arrayListOf(
                        BatchEntityError("urn:ngsi-ld:Device:HCMR-AQUABOX3".toUri(), arrayListOf(""))
                    )
                )
            )

        assertEquals(listOf("urn:ngsi-ld:Device:HCMR-AQUABOX1").toListOfUri(), batchOperationResult.success)
        assertEquals(
            listOf(
                BatchEntityError(
                    "urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri(),
                    arrayListOf("Target entity urn:ngsi-ld:Device:HCMR-AQUABOX3 does not exist.")
                )
            ),
            batchOperationResult.errors
        )
    }

    @Test
    fun `it should count as error updating which results in BadRequestDataException`() {
        val firstEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { firstEntity.id } returns "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
        every { firstEntity.getLinkedEntitiesIds() } returns emptyList()
        val secondEntity = mockkClass(NgsiLdEntity::class, relaxed = true)
        every { secondEntity.id } returns "urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()
        every { secondEntity.getLinkedEntitiesIds() } returns emptyList()

        every { neo4jRepository.filterExistingEntitiesAsIds(emptyList()) } returns emptyList()
        every {
            entityService.appendEntityAttributes(eq("urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()), any(), any())
        } returns UpdateResult(
            emptyList(),
            emptyList()
        )
        every {
            entityService.appendEntityAttributes(eq("urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()), any(), any())
        } throws BadRequestDataException("error")

        val batchOperationResult =
            entityOperationService.update(listOf(firstEntity, secondEntity), BatchOperationResult())

        assertEquals(listOf("urn:ngsi-ld:Device:HCMR-AQUABOX1").toListOfUri(), batchOperationResult.success)
        assertEquals(
            listOf(BatchEntityError("urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri(), arrayListOf("error"))),
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
            entityOperationService.update(listOf(firstEntity, secondEntity), BatchOperationResult())

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
            entityOperationService.replace(listOf(firstEntity, secondEntity), BatchOperationResult())

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
            entityOperationService.replace(listOf(firstEntity, secondEntity), BatchOperationResult())

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

        val batchOperationResult = entityOperationService.replace(
            listOf(firstEntity, secondEntity),
            BatchOperationResult()
        )

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
                    mutableListOf("Failed to delete entity with id urn:ngsi-ld:Device:HCMR-AQUABOX2")
                )
            ),
            batchOperationResult.errors
        )
    }

    @Test
    fun `it should return error messages BatchOperationResult when deletion in DB has failed`() {
        val firstEntity = "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
        val secondEntity = "urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri()

        every {
            entityService.deleteEntity(any())
        } throws RuntimeException("Something went wrong with deletion request")

        val batchOperationResult = entityOperationService.delete(setOf(firstEntity, secondEntity))

        assertEquals(emptyList<String>(), batchOperationResult.success)
        assertEquals(
            listOf(
                BatchEntityError(
                    "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri(),
                    mutableListOf("Failed to delete entity with id urn:ngsi-ld:Device:HCMR-AQUABOX1")
                ),
                BatchEntityError(
                    "urn:ngsi-ld:Device:HCMR-AQUABOX2".toUri(),
                    mutableListOf("Failed to delete entity with id urn:ngsi-ld:Device:HCMR-AQUABOX2")
                )
            ),
            batchOperationResult.errors
        )
    }
}
