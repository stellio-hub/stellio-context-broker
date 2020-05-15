package com.egm.stellio.entity.util

import com.egm.stellio.entity.service.EntityService
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

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [GraphBuilder::class])
@ActiveProfiles("test")
class GraphBuilderTest {

    @MockkBean
    private lateinit var entityService: EntityService

    @Autowired
    private lateinit var graphBuilder: GraphBuilder

    @Test
    fun `it should create graph based on given entities`() {
        val firstEntity = mockkClass(ExpandedEntity::class)
        every { firstEntity.id } returns "1"
        every { firstEntity.getRelationships() } returns listOf()
        val secondEntity = mockkClass(ExpandedEntity::class)
        every { secondEntity.id } returns "2"
        every { secondEntity.getRelationships() } returns listOf()

        val (graph, errors) = graphBuilder.build(listOf(firstEntity, secondEntity))

        assertEquals(setOf(firstEntity, secondEntity), graph.nodes())
        assertTrue(errors.isEmpty())
    }

    @Test
    fun `it should create graph based on given entities with relationships`() {
        val firstEntity = mockkClass(ExpandedEntity::class)
        every { firstEntity.id } returns "1"
        every { firstEntity.getRelationships() } returns listOf("2")
        val secondEntity = mockkClass(ExpandedEntity::class)
        every { secondEntity.id } returns "2"
        every { secondEntity.getRelationships() } returns listOf("1")

        every { entityService.exists(any()) } returns false

        val (graph, errors) = graphBuilder.build(listOf(firstEntity, secondEntity))

        assertEquals(setOf(firstEntity, secondEntity), graph.nodes())
        assertTrue(errors.isEmpty())
    }

    @Test
    fun `it should create graph based on given entities with errors`() {
        val firstEntity = mockkClass(ExpandedEntity::class)
        every { firstEntity.id } returns "1"
        every { firstEntity.getRelationships() } returns listOf()
        val secondEntity = mockkClass(ExpandedEntity::class)
        every { secondEntity.id } returns "2"
        every { secondEntity.getRelationships() } throws BadRequestDataException("Invalid entity")

        val (graph, errors) = graphBuilder.build(listOf(firstEntity, secondEntity))

        assertEquals(setOf(firstEntity), graph.nodes())
        assertEquals(listOf(BatchEntityError("2", arrayListOf("Invalid entity"))), errors)
    }

    @Test
    fun `it should not create entities for which child entity has an invalid relationship`() {
        val firstEntity = mockkClass(ExpandedEntity::class)
        every { firstEntity.id } returns "1"
        every { firstEntity.getRelationships() } returns listOf("2")
        val secondEntity = mockkClass(ExpandedEntity::class)
        every { secondEntity.id } returns "2"
        every { secondEntity.getRelationships() } returns listOf("3")
        val thirdEntity = mockkClass(ExpandedEntity::class)
        every { thirdEntity.id } returns "3"
        every { thirdEntity.getRelationships() } throws BadRequestDataException("Target entity 4 does not exist")

        every { entityService.exists(any()) } returns false

        val (graph, errors) = graphBuilder.build(listOf(firstEntity, secondEntity, thirdEntity))

        assertTrue(graph.nodes().isEmpty())
        assertEquals(
            listOf(
                BatchEntityError("3", arrayListOf("Target entity 4 does not exist")),
                BatchEntityError("2", arrayListOf("Target entity 4 does not exist")),
                BatchEntityError("1", arrayListOf("Target entity 4 does not exist"))
            ), errors
        )
    }
}