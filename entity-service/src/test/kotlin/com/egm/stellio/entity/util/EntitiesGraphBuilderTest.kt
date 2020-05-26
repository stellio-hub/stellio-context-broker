package com.egm.stellio.entity.util

import com.egm.stellio.entity.repository.Neo4jRepository
import com.egm.stellio.entity.web.BatchEntityError
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ExpandedEntity
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import io.mockk.mockkClass
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [EntitiesGraphBuilder::class])
@ActiveProfiles("test")
class EntitiesGraphBuilderTest {

    @MockkBean(relaxed = true)
    private lateinit var neo4jRepository: Neo4jRepository

    @Autowired
    private lateinit var entitiesGraphBuilder: EntitiesGraphBuilder

    @Test
    fun `it should create graph based on given entities`() {
        val firstEntity = mockkClass(ExpandedEntity::class)
        every { firstEntity.id } returns "1"
        every { firstEntity.getLinkedEntitiesIds() } returns listOf()
        val secondEntity = mockkClass(ExpandedEntity::class)
        every { secondEntity.id } returns "2"
        every { secondEntity.getLinkedEntitiesIds() } returns listOf()

        val (graph, errors) = entitiesGraphBuilder.build(listOf(firstEntity, secondEntity))

        assertEquals(setOf(firstEntity, secondEntity), graph.vertexSet())
        assertTrue(errors.isEmpty())
    }

    @Test
    fun `it should create graph based on given entities with relationships`() {
        val firstEntity = mockkClass(ExpandedEntity::class)
        every { firstEntity.id } returns "1"
        every { firstEntity.getLinkedEntitiesIds() } returns listOf("2")
        val secondEntity = mockkClass(ExpandedEntity::class)
        every { secondEntity.id } returns "2"
        every { secondEntity.getLinkedEntitiesIds() } returns listOf("1")

        val (graph, errors) = entitiesGraphBuilder.build(listOf(firstEntity, secondEntity))

        assertEquals(setOf(firstEntity, secondEntity), graph.vertexSet())
        assertTrue(errors.isEmpty())
    }

    @Test
    fun `it should create graph based on given entities with relationships to entities in DB`() {
        val firstEntity = mockkClass(ExpandedEntity::class)
        every { firstEntity.id } returns "1"
        every { firstEntity.getLinkedEntitiesIds() } returns listOf("4")
        val secondEntity = mockkClass(ExpandedEntity::class)
        every { secondEntity.id } returns "2"
        every { secondEntity.getLinkedEntitiesIds() } returns listOf("3")

        every { neo4jRepository.filterExistingEntitiesIds(listOf("4")) } returns listOf("4")
        every { neo4jRepository.filterExistingEntitiesIds(listOf("3")) } returns listOf("3")

        val (graph, errors) = entitiesGraphBuilder.build(listOf(firstEntity, secondEntity))

        assertEquals(setOf(firstEntity, secondEntity), graph.vertexSet())
        assertTrue(errors.isEmpty())
    }

    @Test
    fun `it should create graph based on given entities with errors`() {
        val firstEntity = mockkClass(ExpandedEntity::class)
        every { firstEntity.id } returns "1"
        every { firstEntity.getLinkedEntitiesIds() } returns listOf()
        val secondEntity = mockkClass(ExpandedEntity::class)
        every { secondEntity.id } returns "2"
        every { secondEntity.getLinkedEntitiesIds() } throws BadRequestDataException("Invalid entity")

        val (graph, errors) = entitiesGraphBuilder.build(listOf(firstEntity, secondEntity))

        assertEquals(setOf(firstEntity), graph.vertexSet())
        assertEquals(listOf(BatchEntityError("2", arrayListOf("Invalid entity"))), errors)
    }

    @Test
    fun `it should not create entities for which child entity has an invalid relationship`() {
        val firstEntity = mockkClass(ExpandedEntity::class)
        every { firstEntity.id } returns "1"
        every { firstEntity.getLinkedEntitiesIds() } returns listOf("2")
        val secondEntity = mockkClass(ExpandedEntity::class)
        every { secondEntity.id } returns "2"
        every { secondEntity.getLinkedEntitiesIds() } returns listOf("3")
        val thirdEntity = mockkClass(ExpandedEntity::class)
        every { thirdEntity.id } returns "3"
        every { thirdEntity.getLinkedEntitiesIds() } throws BadRequestDataException("Target entity 4 does not exist")

        val (graph, errors) = entitiesGraphBuilder.build(listOf(firstEntity, secondEntity, thirdEntity))

        assertTrue(graph.vertexSet().isEmpty())
        assertEquals(
            listOf(
                BatchEntityError("3", arrayListOf("Target entity 4 does not exist")),
                BatchEntityError(
                    "2",
                    arrayListOf("Target entity 3 failed to be created because of an invalid relationship.")
                ),
                BatchEntityError(
                    "1",
                    arrayListOf("Target entity 2 failed to be created because of an invalid relationship.")
                )
            ), errors
        )
    }
}