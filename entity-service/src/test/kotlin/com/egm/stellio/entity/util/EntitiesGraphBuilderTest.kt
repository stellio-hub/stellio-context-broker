package com.egm.stellio.entity.util

import com.egm.stellio.entity.repository.Neo4jRepository
import com.egm.stellio.entity.web.BatchEntityError
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.util.toUri
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
        val firstEntity = mockkClass(NgsiLdEntity::class)
        every { firstEntity.id } returns "1".toUri()
        every { firstEntity.getLinkedEntitiesIds() } returns listOf()
        val secondEntity = mockkClass(NgsiLdEntity::class)
        every { secondEntity.id } returns "2".toUri()
        every { secondEntity.getLinkedEntitiesIds() } returns listOf()

        val (graph, errors) = entitiesGraphBuilder.build(listOf(firstEntity, secondEntity))

        assertEquals(setOf(firstEntity, secondEntity), graph.vertexSet())
        assertTrue(errors.isEmpty())
    }

    @Test
    fun `it should create graph based on given entities with relationships`() {
        val firstEntity = mockkClass(NgsiLdEntity::class)
        every { firstEntity.id } returns "1".toUri()
        every { firstEntity.getLinkedEntitiesIds() } returns listOf("2".toUri())
        val secondEntity = mockkClass(NgsiLdEntity::class)
        every { secondEntity.id } returns "2".toUri()
        every { secondEntity.getLinkedEntitiesIds() } returns listOf("1".toUri())

        val (graph, errors) = entitiesGraphBuilder.build(listOf(firstEntity, secondEntity))

        assertEquals(setOf(firstEntity, secondEntity), graph.vertexSet())
        assertTrue(errors.isEmpty())
    }

    @Test
    fun `it should create graph based on given entities with relationships to entities in DB`() {
        val firstEntity = mockkClass(NgsiLdEntity::class)
        every { firstEntity.id } returns "1".toUri()
        every { firstEntity.getLinkedEntitiesIds() } returns listOf("4".toUri())
        val secondEntity = mockkClass(NgsiLdEntity::class)
        every { secondEntity.id } returns "2".toUri()
        every { secondEntity.getLinkedEntitiesIds() } returns listOf("3".toUri())

        every { neo4jRepository.filterExistingEntitiesAsIds(listOf("4".toUri())) } returns listOf("4".toUri())
        every { neo4jRepository.filterExistingEntitiesAsIds(listOf("3".toUri())) } returns listOf("3".toUri())

        val (graph, errors) = entitiesGraphBuilder.build(listOf(firstEntity, secondEntity))

        assertEquals(setOf(firstEntity, secondEntity), graph.vertexSet())
        assertTrue(errors.isEmpty())
    }

    @Test
    fun `it should create graph based on given entities with errors`() {
        val firstEntity = mockkClass(NgsiLdEntity::class)
        every { firstEntity.id } returns "1".toUri()
        every { firstEntity.getLinkedEntitiesIds() } returns listOf()
        val secondEntity = mockkClass(NgsiLdEntity::class)
        every { secondEntity.id } returns "2".toUri()
        every { secondEntity.getLinkedEntitiesIds() } throws BadRequestDataException("Invalid entity")

        val (graph, errors) = entitiesGraphBuilder.build(listOf(firstEntity, secondEntity))

        assertEquals(setOf(firstEntity), graph.vertexSet())
        assertEquals(listOf(BatchEntityError("2".toUri(), arrayListOf("Invalid entity"))), errors)
    }

    @Test
    fun `it should not create entities for which child entity has an invalid relationship`() {
        val firstEntity = mockkClass(NgsiLdEntity::class)
        every { firstEntity.id } returns "1".toUri()
        every { firstEntity.getLinkedEntitiesIds() } returns listOf("2".toUri())
        val secondEntity = mockkClass(NgsiLdEntity::class)
        every { secondEntity.id } returns "2".toUri()
        every { secondEntity.getLinkedEntitiesIds() } returns listOf("3".toUri())
        val thirdEntity = mockkClass(NgsiLdEntity::class)
        every { thirdEntity.id } returns "3".toUri()
        every { thirdEntity.getLinkedEntitiesIds() } throws BadRequestDataException("Target entity 4 does not exist")

        val (graph, errors) = entitiesGraphBuilder.build(listOf(firstEntity, secondEntity, thirdEntity))

        assertTrue(graph.vertexSet().isEmpty())
        assertEquals(
            listOf(
                BatchEntityError("3".toUri(), arrayListOf("Target entity 4 does not exist")),
                BatchEntityError(
                    "2".toUri(),
                    arrayListOf("Target entity 3 failed to be created because of an invalid relationship")
                ),
                BatchEntityError(
                    "1".toUri(),
                    arrayListOf("Target entity 2 failed to be created because of an invalid relationship")
                )
            ),
            errors
        )
    }
}
