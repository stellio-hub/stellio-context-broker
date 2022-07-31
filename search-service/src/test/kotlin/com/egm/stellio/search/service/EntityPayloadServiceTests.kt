package com.egm.stellio.search.service

import com.egm.stellio.search.model.EntityPayload
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.InternalErrorException
import com.egm.stellio.shared.util.shouldSucceed
import com.egm.stellio.shared.util.toUri
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.test.context.ActiveProfiles

@OptIn(ExperimentalCoroutinesApi::class)
@SpringBootTest
@ActiveProfiles("test")
class EntityPayloadServiceTests : WithTimescaleContainer {

    @Autowired
    private lateinit var entityPayloadService: EntityPayloadService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    private val entityUri = "urn:ngsi-ld:Entity:01".toUri()
    private val emptyJsonPayload = "{}"

    @AfterEach
    fun clearEntityAccessRightsTable() {
        r2dbcEntityTemplate.delete(EntityPayload::class.java)
            .all()
            .block()
    }

    @Test
    fun `it should create an entity payload if none existed yet`() = runTest {
        entityPayloadService.createEntityPayload(entityUri, emptyJsonPayload)
            .shouldSucceed()
    }

    @Test
    fun `it should not create an entity payload if one already existed`() = runTest {
        entityPayloadService.createEntityPayload(entityUri, emptyJsonPayload)

        entityPayloadService.createEntityPayload(entityUri, emptyJsonPayload)
            .fold({
                assertThat(it).isExactlyInstanceOf(InternalErrorException::class.java)
            }, {})
    }

    @Test
    fun `it should upsert an entity payload if one already existed`() = runTest {
        entityPayloadService.createEntityPayload(entityUri, emptyJsonPayload)

        entityPayloadService.upsertEntityPayload(entityUri, emptyJsonPayload)
            .shouldSucceed()
    }

    @Test
    fun `it should delete an entity payload`() = runTest {
        entityPayloadService.createEntityPayload(entityUri, emptyJsonPayload)

        val deleteResult = entityPayloadService.deleteEntityPayload(entityUri)
        assertTrue(deleteResult.isRight())

        // if correctly deleted, we should be able to create a new one
        val createResult = entityPayloadService.createEntityPayload(entityUri, emptyJsonPayload)
        assertTrue(createResult.isRight())
    }
}
