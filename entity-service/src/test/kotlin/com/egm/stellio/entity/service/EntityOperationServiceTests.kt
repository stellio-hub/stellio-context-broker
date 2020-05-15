package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.UpdateResult
import com.egm.stellio.entity.util.GraphBuilder
import com.egm.stellio.entity.web.BatchEntityError
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ExpandedEntity
import com.ninjasquad.springmockk.MockkBean
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockkClass
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
    private lateinit var graphBuilder: GraphBuilder

    @Autowired
    private lateinit var entityOperationService: EntityOperationService

    @Test
    fun `it should split entities per existence`() {
        val firstEntity = mockkClass(ExpandedEntity::class)
        every { firstEntity.id } returns "1"
        val secondEntity = mockkClass(ExpandedEntity::class)
        every { secondEntity.id } returns "2"

        every { entityService.exists("1") } returns true
        every { entityService.exists("2") } returns false

        val (exist, doNotExist) = entityOperationService.splitEntitiesByExistence(listOf(firstEntity, secondEntity))

        assertEquals(listOf(firstEntity), exist)
        assertEquals(listOf(secondEntity), doNotExist)
    }

    @Test
    fun `it should split empty list`() {
        val (exist, doNotExist) = entityOperationService.splitEntitiesByExistence(listOf())

        assertTrue(exist.isEmpty())
        assertTrue(doNotExist.isEmpty())
    }

    @Test
    fun `it should create naively isolated entities`() {
        val firstEntity = mockkClass(ExpandedEntity::class)
        every { firstEntity.id } returns "1"
        val secondEntity = mockkClass(ExpandedEntity::class)
        every { secondEntity.id } returns "2"

        val acyclicGraph = com.google.common.graph.GraphBuilder.directed().build<ExpandedEntity>()
        acyclicGraph.addNode(firstEntity)
        acyclicGraph.addNode(secondEntity)

        every { graphBuilder.build(listOf(firstEntity, secondEntity)) } returns Pair(acyclicGraph, listOf())

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

        val acyclicGraph = com.google.common.graph.GraphBuilder.directed().build<ExpandedEntity>()
        acyclicGraph.addNode(firstEntity)
        acyclicGraph.addNode(secondEntity)

        every { graphBuilder.build(listOf(firstEntity, secondEntity)) } returns Pair(acyclicGraph, listOf())

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

        val cyclicGraph = com.google.common.graph.GraphBuilder.directed().build<ExpandedEntity>()
        cyclicGraph.addNode(firstEntity)
        cyclicGraph.addNode(secondEntity)
        cyclicGraph.putEdge(firstEntity, secondEntity)
        cyclicGraph.putEdge(secondEntity, firstEntity)

        every { graphBuilder.build(listOf(firstEntity, secondEntity)) } returns Pair(cyclicGraph, listOf())

        every { entityService.createTempEntityInBatch(eq("1"), any(), any()) } returns mockkClass(Entity::class)
        every { entityService.createTempEntityInBatch(eq("2"), any(), any()) } returns mockkClass(Entity::class)
        every { entityService.appendEntityAttributes(eq("1"), any(), any()) } returns mockkClass(UpdateResult::class)
        every { entityService.appendEntityAttributes(eq("2"), any(), any()) } returns mockkClass(UpdateResult::class)
        every { entityService.publishCreationEvent(any()) } just Runs

        val batchOperationResult = entityOperationService.create(listOf(firstEntity, secondEntity))

        assertEquals(arrayListOf("1", "2"), batchOperationResult.success)
        assertTrue(batchOperationResult.errors.isEmpty())
    }
}