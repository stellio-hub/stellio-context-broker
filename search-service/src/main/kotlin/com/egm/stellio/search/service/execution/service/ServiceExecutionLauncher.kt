package com.egm.stellio.search.service.execution.service

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.service.execution.model.ServiceExecution
import com.egm.stellio.search.service.execution.model.ServiceExecutionStatus
import com.egm.stellio.search.service.registration.model.ServiceRegistration
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadGatewayException
import com.egm.stellio.shared.model.GatewayTimeoutException
import com.egm.stellio.shared.model.NotImplementedException
import com.egm.stellio.shared.util.DataTypes
import com.egm.stellio.shared.util.ErrorMessages.ServiceExecutionErrorMessages.serviceEndpointContactErrorMessage
import com.egm.stellio.shared.util.ErrorMessages.ServiceExecutionErrorMessages.serviceEndpointErrorMessage
import com.egm.stellio.shared.util.ErrorMessages.ServiceExecutionErrorMessages.serviceExecutionCancellationNotImplementedMessage
import com.egm.stellio.shared.util.ngsiLdDateTime
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

    suspend fun invoke(
        serviceExecution: ServiceExecution,
        serviceRegistration: ServiceRegistration
    ): ServiceExecution =
        invokeEndpoint(serviceExecution, serviceRegistration)
            .fold(
                {
                    serviceExecution.copy(
                        executionStatus = ServiceExecutionStatus.FAILURE,
                        modifiedAt = ngsiLdDateTime()
                    )
                },
                { output ->
                    serviceExecution.copy(
                        executionStatus = ServiceExecutionStatus.SUCCESS,
                        output = output,
                        modifiedAt = ngsiLdDateTime()
                    )
                }
            )

    suspend fun invokeAsynchronously(
        serviceExecution: ServiceExecution,
        serviceRegistration: ServiceRegistration
    ): ServiceExecution = invokeEndpoint(serviceExecution, serviceRegistration)
        .fold(
            {
                serviceExecution.copy(
                    executionStatus = ServiceExecutionStatus.FAILURE,
                    modifiedAt = ngsiLdDateTime()
                )
            },
            { output ->
                serviceExecution.copy(
                    executionStatus = ServiceExecutionStatus.EXECUTING,
                    output = output,
                    modifiedAt = ngsiLdDateTime()
                )
            }
        )

    private suspend fun invokeEndpoint(
        serviceExecution: ServiceExecution,
        serviceRegistration: ServiceRegistration
    ): Either<APIException, Any?> =
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

            if (statusCode.is2xxSuccessful) {
                parseOutput(responseBody).right()
            } else {
                BadGatewayException(
                    serviceEndpointErrorMessage(
                        serviceRegistration.id,
                        serviceRegistration.endpoint,
                        statusCode.value()
                    ),
                    responseBody
                ).left()
            }
        } catch (exception: WebClientRequestException) {
            GatewayTimeoutException(
                serviceEndpointContactErrorMessage(serviceRegistration.id, serviceRegistration.endpoint),
                exception.message
            ).left()
        }

    private fun parseOutput(responseBody: String?): Any? =
        responseBody
            ?.takeUnless(String::isBlank)
            ?.let { body ->
                runCatching { DataTypes.mapper.readValue(body, Any::class.java) }
                    .getOrElse { body }
            }

    suspend fun cancelExecution(serviceExecutionId: URI): Either<APIException, Unit> =
        NotImplementedException(serviceExecutionCancellationNotImplementedMessage(serviceExecutionId)).left()
}
