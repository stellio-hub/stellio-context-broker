package com.egm.stellio.search.service.execution.service

import com.egm.stellio.search.service.execution.model.ServiceExecution
import com.egm.stellio.search.service.execution.model.ServiceExecutionStatus
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.shouldFail
import com.egm.stellio.shared.util.shouldSucceed
import com.egm.stellio.shared.util.shouldSucceedWith
import com.egm.stellio.shared.util.toUri
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertInstanceOf
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.delete
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource

@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = ["application.authentication.enabled=false"])
class ServiceExecutionServiceTests : WithTimescaleContainer, WithKafkaContainer() {
    @Autowired
    private lateinit var serviceExecutionService: ServiceExecutionService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    @AfterEach
    fun deleteServiceExecutions() {
        r2dbcEntityTemplate.delete<ServiceExecution>()
            .all()
            .block()
    }

    @Test
    fun `create and retrieve should preserve the complete execution`() = runTest {
        val execution = buildExecution().copy(
            executionStatus = ServiceExecutionStatus.SUCCESS,
            progress = 1.0,
            output = "Brightness successfully changed.",
            responseStatusCode = 200
        )

        serviceExecutionService.create(execution).shouldSucceed()

        serviceExecutionService.getById(execution.id).shouldSucceedWith {
            assertEquals(execution.id, it.id)
            assertEquals(execution.serviceId, it.serviceId)
            assertEquals(execution.entityId, it.entityId)
            assertEquals(execution.entityType, it.entityType)
            assertEquals(execution.serviceName, it.serviceName)
            assertEquals(execution.input, it.input)
            assertEquals(ServiceExecutionStatus.SUCCESS, it.executionStatus)
            assertEquals(1.0, it.progress)
            assertEquals(execution.output, it.output)
            assertEquals(200, it.responseStatusCode)
        }
    }

    @Test
    fun `create should reject an existing id`() = runTest {
        val execution = buildExecution()

        serviceExecutionService.create(execution).shouldSucceed()
        serviceExecutionService.create(execution).shouldFail {
            assertInstanceOf<AlreadyExistsException>(it)
        }
    }

    @Test
    fun `merge and delete should complete the lifecycle`() = runTest {
        val execution = buildExecution()
        serviceExecutionService.create(execution).shouldSucceed()

        serviceExecutionService.merge(
            execution.id,
            mapOf(
                "executionStatus" to "executing",
                "progress" to 0.4,
                "output" to "Brightness is changing."
            ),
            emptyList()
        ).shouldSucceed()
        serviceExecutionService.getById(execution.id).shouldSucceedWith {
            assertEquals(ServiceExecutionStatus.EXECUTING, it.executionStatus)
            assertEquals(0.4, it.progress)
            assertEquals("Brightness is changing.", it.output)
        }

        serviceExecutionService.delete(execution.id).shouldSucceed()
        serviceExecutionService.getById(execution.id).shouldFail {
            assertInstanceOf<ResourceNotFoundException>(it)
        }
    }

    @Test
    fun `merge should reject changes to non executor-controlled members`() = runTest {
        val execution = buildExecution()
        serviceExecutionService.create(execution).shouldSucceed()

        serviceExecutionService.merge(
            execution.id,
            mapOf("entityId" to "urn:ngsi-ld:Light:002"),
            emptyList()
        ).shouldFail {
            assertInstanceOf<BadRequestDataException>(it)
        }

        serviceExecutionService.getById(execution.id).shouldSucceedWith {
            assertEquals(execution.entityId, it.entityId)
        }
    }

    private fun buildExecution(idSuffix: String = "4673") =
        ServiceExecution(
            id = "urn:ngsi-ld:ServiceExecution:$idSuffix".toUri(),
            serviceId = "urn:ngsi-ld:ServiceRegistration:sr3689".toUri(),
            entityId = "urn:ngsi-ld:Light:001".toUri(),
            entityType = "Light",
            input = 125
        )
}
