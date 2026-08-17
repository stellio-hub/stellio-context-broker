package com.egm.stellio.search.service.execution.service

import arrow.core.Either
import arrow.core.left
import com.egm.stellio.search.service.execution.model.ServiceExecution
import com.egm.stellio.search.service.execution.model.ServiceExecutionStatus
import com.egm.stellio.search.service.registration.model.ServiceRegistration
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.GatewayTimeoutException
import com.egm.stellio.shared.model.NotImplementedException
import com.egm.stellio.shared.util.DataTypes
import com.egm.stellio.shared.util.ErrorMessages.ServiceExecution.serviceEndpointContactErrorMessage
import com.egm.stellio.shared.util.ErrorMessages.ServiceExecution.serviceExecutionCancellationNotImplementedMessage
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.ngsiLdDateTime
import org.springframework.http.HttpStatus
import org.springframework.http.HttpStatusCode
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientRequestException
import org.springframework.web.reactive.function.client.awaitBodyOrNull
import org.springframework.web.reactive.function.client.awaitExchange
import java.net.URI

@Component
class ServiceExecutionLauncher {
    private val webClient = WebClient.create()

    suspend fun invokeService(
        serviceExecution: ServiceExecution,
        serviceRegistration: ServiceRegistration
    ): ServiceExecution {
        val (responseStatusCode, output) = invokeEndpoint(serviceExecution, serviceRegistration)

        return serviceExecution.copy(
            executionStatus = if (responseStatusCode.is2xxSuccessful) {
                ServiceExecutionStatus.SUCCESS
            } else {
                ServiceExecutionStatus.FAILURE
            },
            output = output,
            responseStatusCode = responseStatusCode.value(),
            modifiedAt = ngsiLdDateTime()
        )
    }

    /** Invokes a service whose work is asynchronous and waits for its acknowledgement response. */
    suspend fun invokeAsynchronousService(
        serviceExecution: ServiceExecution,
        serviceRegistration: ServiceRegistration
    ): ServiceExecution {
        val (responseStatusCode, output) = invokeEndpoint(serviceExecution, serviceRegistration)
        return serviceExecution.copy(
            executionStatus = if (responseStatusCode.is2xxSuccessful) {
                ServiceExecutionStatus.EXECUTING
            } else {
                ServiceExecutionStatus.FAILURE
            },
            output = output,
            responseStatusCode = responseStatusCode.value(),
            modifiedAt = ngsiLdDateTime()
        )
    }

    private suspend fun invokeEndpoint(
        serviceExecution: ServiceExecution,
        serviceRegistration: ServiceRegistration
    ): Pair<HttpStatusCode, Any?> =
        try {
            val request = webClient
                .method(serviceRegistration.endpointMethod)
                .uri(serviceRegistration.endpoint)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .bodyValue(DataTypes.serialize(serviceExecution.input))

            val (statusCode, responseBody) = request.awaitExchange { response ->
                response.statusCode() to response.awaitBodyOrNull<String>()
            }

            statusCode to parseOutput(responseBody)
        } catch (exception: WebClientRequestException) {
            HttpStatus.GATEWAY_TIMEOUT to GatewayTimeoutException(
                serviceEndpointContactErrorMessage(serviceRegistration.id, serviceRegistration.endpoint),
                exception.message
            ).toProblemDetail()
        }

    private fun parseOutput(responseBody: String?): Any? =
        responseBody
            ?.takeUnless(String::isBlank)
            ?.let { body ->
                runCatching { deserializeAs<Any>(body) }.getOrElse { body }
            }

    suspend fun cancelExecution(serviceExecutionId: URI): Either<APIException, Unit> =
        NotImplementedException(serviceExecutionCancellationNotImplementedMessage(serviceExecutionId)).left()
}
