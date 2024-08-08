package com.egm.stellio.search.entity.service

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.support.*
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.*
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.net.URI

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [EntityQueryService::class])
@ActiveProfiles("test")
@ExperimentalCoroutinesApi
class EntityQueryServiceTests {

    @Autowired
    private lateinit var queryService: EntityQueryService

    @MockkBean
    private lateinit var entityPayloadService: EntityPayloadService

    private val entityUri = "urn:ngsi-ld:BeeHive:TESTC".toUri()

    @Test
    fun `it should return a JSON-LD entity when querying by id`() = runTest {
        coEvery { entityPayloadService.retrieve(any<URI>()) } returns gimmeEntityPayload().right()

        queryService.queryEntity(entityUri)
            .shouldSucceedWith {
                assertEquals(entityUri.toString(), it.id)
                assertEquals(listOf(BEEHIVE_TYPE), it.types)
                assertEquals(7, it.members.size)
            }
    }

    @Test
    fun `it should return an API exception if no entity exists with the given id`() = runTest {
        coEvery { entityPayloadService.retrieve(any<URI>()) } returns ResourceNotFoundException("").left()

        queryService.queryEntity(entityUri)
            .shouldFail {
                assertTrue(it is ResourceNotFoundException)
            }
    }

    @Test
    fun `it should return a list of JSON-LD entities when querying entities`() = runTest {
        coEvery { entityPayloadService.queryEntities(any(), any()) } returns listOf(entityUri)
        coEvery { entityPayloadService.queryEntitiesCount(any(), any()) } returns 1.right()
        coEvery { entityPayloadService.retrieve(any<List<URI>>()) } returns listOf(gimmeEntityPayload())

        queryService.queryEntities(buildDefaultQueryParams()) { null }
            .shouldSucceedWith {
                assertEquals(1, it.second)
                assertEquals(entityUri.toString(), it.first[0].id)
                assertEquals(listOf(BEEHIVE_TYPE), it.first[0].types)
                assertEquals(7, it.first[0].members.size)
            }
    }

    @Test
    fun `it should return an empty list if no entity matched the query`() = runTest {
        coEvery { entityPayloadService.queryEntities(any(), any()) } returns emptyList()
        coEvery { entityPayloadService.queryEntitiesCount(any(), any()) } returns 0.right()

        queryService.queryEntities(buildDefaultQueryParams()) { null }
            .shouldSucceedWith {
                assertEquals(0, it.second)
                assertTrue(it.first.isEmpty())
            }
    }

    private fun gimmeEntityPayload() =
        gimmeEntityPayload(
            entityId = entityUri,
            payload = loadSampleData("beehive_expanded.jsonld")
        )
}
