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
import com.egm.stellio.shared.model.ExpandedEntity
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
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
        val firstEntity = mockkClass(ExpandedEntity::class)
        every { firstEntity.id } returns "1"
        val secondEntity = mockkClass(ExpandedEntity::class)
        every { secondEntity.id } returns "2"

        every { neo4jRepository.filterExistingEntitiesIds(listOf("1", "2")) } returns listOf("1")

        val (exist, doNotExist) = entityOperationService.splitEntitiesByExistence(listOf(firstEntity, secondEntity))

        assertEquals(listOf(firstEntity), exist)
        assertEquals(listOf(secondEntity), doNotExist)
    }

    @Test
    fun `it should create naively isolated entities`() {
        val firstEntity = mockkClass(ExpandedEntity::class)
        every { firstEntity.id } returns "1"
        val secondEntity = mockkClass(ExpandedEntity::class)
        every { secondEntity.id } returns "2"

        val acyclicGraph = DirectedPseudograph<ExpandedEntity, DefaultEdge>(DefaultEdge::class.java)
        acyclicGraph.addVertex(firstEntity)
        acyclicGraph.addVertex(secondEntity)

        every { entitiesGraphBuilder.build(listOf(firstEntity, secondEntity)) } returns Pair(acyclicGraph, listOf())

        every { entityService.createEntity(firstEntity) } returns mockkClass(Entity::class)
        every { entityService.createEntity(secondEntity) } returns mockkClass(Entity::class)

        val batchOperationResult = entityOperationService.create(listOf(firstEntity, secondEntity))

        assertEquals(arrayListOf("1", "2"), batchOperationResult.success)
        assertTrue(batchOperationResult.errors.isEmpty())
    }

    @Test
    fun `it should create naively isolated entities with an error`() {
        val firstEntity = mockkClass(ExpandedEntity::class)
        every { firstEntity.id } returns "1"
        val secondEntity = mockkClass(ExpandedEntity::class)
        every { secondEntity.id } returns "2"

        val acyclicGraph = DirectedPseudograph<ExpandedEntity, DefaultEdge>(DefaultEdge::class.java)
        acyclicGraph.addVertex(firstEntity)
        acyclicGraph.addVertex(secondEntity)

        every { entitiesGraphBuilder.build(listOf(firstEntity, secondEntity)) } returns Pair(acyclicGraph, listOf())

        every { entityService.createEntity(firstEntity) } returns mockkClass(Entity::class)
        every { entityService.createEntity(secondEntity) } throws BadRequestDataException("Invalid entity")

        val batchOperationResult = entityOperationService.create(listOf(firstEntity, secondEntity))

        assertEquals(arrayListOf("1"), batchOperationResult.success)
        assertEquals(arrayListOf(BatchEntityError("2", arrayListOf("Invalid entity"))), batchOperationResult.errors)
    }

    @Test
    fun `it should create entities with cyclic dependencies`() {
        val firstEntity = mockkClass(ExpandedEntity::class, relaxed = true)
        every { firstEntity.id } returns "1"
        val secondEntity = mockkClass(ExpandedEntity::class, relaxed = true)
        every { secondEntity.id } returns "2"

        val cyclicGraph = DirectedPseudograph<ExpandedEntity, DefaultEdge>(DefaultEdge::class.java)
        cyclicGraph.addVertex(firstEntity)
        cyclicGraph.addVertex(secondEntity)
        cyclicGraph.addEdge(firstEntity, secondEntity)
        cyclicGraph.addEdge(secondEntity, firstEntity)

        every { entitiesGraphBuilder.build(listOf(firstEntity, secondEntity)) } returns Pair(cyclicGraph, listOf())
        every { entityService.appendEntityAttributes(eq("1"), any(), any()) } returns mockkClass(UpdateResult::class)
        every { entityService.appendEntityAttributes(eq("2"), any(), any()) } returns mockkClass(UpdateResult::class)
        every { entityService.publishCreationEvent(any()) } just Runs
        every { entityRepository.save<Entity>(any()) } returns mockk()

        val batchOperationResult = entityOperationService.create(listOf(firstEntity, secondEntity))

        assertEquals(arrayListOf("1", "2"), batchOperationResult.success)
        assertTrue(batchOperationResult.errors.isEmpty())
    }

    @Test
    fun `it should not update entities with relationships to invalid entity not found in DB`() {
        val firstEntity = mockkClass(ExpandedEntity::class, relaxed = true)
        every { firstEntity.id } returns "1"
        every { firstEntity.getLinkedEntitiesIds() } returns emptyList()
        val secondEntity = mockkClass(ExpandedEntity::class, relaxed = true)
        every { secondEntity.id } returns "2"
        every { secondEntity.getLinkedEntitiesIds() } returns listOf("3")

        every { neo4jRepository.filterExistingEntitiesIds(listOf()) } returns emptyList()
        every { neo4jRepository.filterExistingEntitiesIds(listOf("3")) } returns emptyList()
        every { entityService.appendEntityAttributes(eq("1"), any(), any()) } returns UpdateResult(
            emptyList(),
            emptyList()
        )

        val batchOperationResult =
            entityOperationService.update(listOf(firstEntity, secondEntity), BatchOperationResult())

        assertEquals(listOf("1"), batchOperationResult.success)
        assertEquals(
            listOf(BatchEntityError("2", arrayListOf("Target entity 3 does not exist."))),
            batchOperationResult.errors
        )
    }

    @Test
    fun `it should not update entities with relationships to invalid entity given in BatchOperationResult`() {
        val firstEntity = mockkClass(ExpandedEntity::class, relaxed = true)
        every { firstEntity.id } returns "1"
        every { firstEntity.getLinkedEntitiesIds() } returns emptyList()
        val secondEntity = mockkClass(ExpandedEntity::class, relaxed = true)
        every { secondEntity.id } returns "2"
        every { secondEntity.getLinkedEntitiesIds() } returns listOf("3")

        every { neo4jRepository.filterExistingEntitiesIds(listOf()) } returns emptyList()
        every { entityService.appendEntityAttributes(eq("1"), any(), any()) } returns UpdateResult(
            emptyList(),
            emptyList()
        )

        val batchOperationResult =
            entityOperationService.update(
                listOf(firstEntity, secondEntity),
                BatchOperationResult(
                    errors = arrayListOf(BatchEntityError("3", arrayListOf("")))
                )
            )

        assertEquals(listOf("1"), batchOperationResult.success)
        assertEquals(
            listOf(BatchEntityError("2", arrayListOf("Target entity 3 does not exist."))),
            batchOperationResult.errors
        )
    }

    @Test
    fun `it should count as error updating which results in BadRequestDataException`() {
        val firstEntity = mockkClass(ExpandedEntity::class, relaxed = true)
        every { firstEntity.id } returns "1"
        every { firstEntity.getLinkedEntitiesIds() } returns emptyList()
        val secondEntity = mockkClass(ExpandedEntity::class, relaxed = true)
        every { secondEntity.id } returns "2"
        every { secondEntity.getLinkedEntitiesIds() } returns emptyList()

        every { neo4jRepository.filterExistingEntitiesIds(emptyList()) } returns emptyList()
        every { entityService.appendEntityAttributes(eq("1"), any(), any()) } returns UpdateResult(
            emptyList(),
            emptyList()
        )
        every { entityService.appendEntityAttributes(eq("2"), any(), any()) } throws BadRequestDataException("error")

        val batchOperationResult =
            entityOperationService.update(listOf(firstEntity, secondEntity), BatchOperationResult())

        val a = listOf(1, 2)

        assertEquals(listOf("1"), batchOperationResult.success)
        assertEquals(
            listOf(BatchEntityError("2", arrayListOf("error"))),
            batchOperationResult.errors
        )
    }

    @Test
    fun `it should count as error not updated attributes in entities`() {
        val firstEntity = mockkClass(ExpandedEntity::class, relaxed = true)
        every { firstEntity.id } returns "1"
        every { firstEntity.getLinkedEntitiesIds() } returns emptyList()
        val secondEntity = mockkClass(ExpandedEntity::class, relaxed = true)
        every { secondEntity.id } returns "2"
        every { secondEntity.getLinkedEntitiesIds() } returns emptyList()

        every { neo4jRepository.filterExistingEntitiesIds(listOf()) } returns emptyList()
        every { entityService.appendEntityAttributes(eq("1"), any(), any()) } returns UpdateResult(
            emptyList(),
            emptyList()
        )
        every { entityService.appendEntityAttributes(eq("2"), any(), any()) } returns UpdateResult(
            emptyList(),
            listOf(
                NotUpdatedDetails("attribute#1", "reason"),
                NotUpdatedDetails("attribute#2", "reason")
            )
        )

        val batchOperationResult =
            entityOperationService.update(listOf(firstEntity, secondEntity), BatchOperationResult())

        assertEquals(listOf("1"), batchOperationResult.success)
        assertEquals(
            listOf(BatchEntityError("2", arrayListOf("attribute#1 : reason", "attribute#2 : reason"))),
            batchOperationResult.errors
        )
    }

    @Test
    fun `it should replace entities`() {
        val firstEntity = mockkClass(ExpandedEntity::class, relaxed = true)
        every { firstEntity.id } returns "1"
        every { firstEntity.getLinkedEntitiesIds() } returns listOf()
        val secondEntity = mockkClass(ExpandedEntity::class, relaxed = true)
        every { secondEntity.id } returns "2"
        every { secondEntity.getLinkedEntitiesIds() } returns listOf()

        every { neo4jRepository.filterExistingEntitiesIds(listOf()) } returns listOf()
        every { neo4jRepository.deleteEntityAttributes("1") } returns mockk()
        every { entityService.appendEntityAttributes(eq("1"), any(), any()) } returns mockk()
        every { neo4jRepository.deleteEntityAttributes("2") } returns mockk()
        every { entityService.appendEntityAttributes(eq("2"), any(), any()) } returns mockk()

        val batchOperationResult =
            entityOperationService.replace(listOf(firstEntity, secondEntity), BatchOperationResult())

        assertEquals(listOf("1", "2"), batchOperationResult.success)
        assertTrue(batchOperationResult.errors.isEmpty())
    }

    @Test
    fun `it should count as error entities that couldn't be replaced`() {
        val firstEntity = mockkClass(ExpandedEntity::class, relaxed = true)
        every { firstEntity.id } returns "1"
        every { firstEntity.getLinkedEntitiesIds() } returns listOf()
        val secondEntity = mockkClass(ExpandedEntity::class, relaxed = true)
        every { secondEntity.id } returns "2"
        every { secondEntity.getLinkedEntitiesIds() } returns listOf()

        every { neo4jRepository.filterExistingEntitiesIds(listOf()) } returns listOf()
        every { neo4jRepository.deleteEntityAttributes("1") } returns mockk()
        every { entityService.appendEntityAttributes(eq("1"), any(), any()) } returns mockk()
        every { neo4jRepository.deleteEntityAttributes("2") } returns mockk()
        every { entityService.appendEntityAttributes(eq("2"), any(), any()) } throws BadRequestDataException("error")

        val batchOperationResult =
            entityOperationService.replace(listOf(firstEntity, secondEntity), BatchOperationResult())

        assertEquals(listOf("1"), batchOperationResult.success)
        assertEquals(
            listOf(BatchEntityError("2", arrayListOf("error"))),
            batchOperationResult.errors
        )
    }
}