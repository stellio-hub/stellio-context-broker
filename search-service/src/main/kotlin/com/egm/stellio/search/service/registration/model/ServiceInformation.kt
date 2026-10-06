package com.egm.stellio.search.service.registration.model

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.raise.ensure
import com.egm.stellio.search.service.util.ServiceSchemaValidator
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.ErrorMessages.ServiceRegistration.SERVICE_INFORMATION_NAME_REQUIRED_MESSAGE
import com.fasterxml.jackson.annotation.JsonProperty
import tools.jackson.databind.JsonNode

data class ServiceInformation(
    val name: String = "",
    val title: String? = null,
    val description: String? = null,
    val mode: ServiceMode? = null,
    val input: JsonNode? = null,
    val output: JsonNode? = null
) {
    fun validate(): Either<APIException, Unit> = either {
        ensure(name.isNotBlank()) { BadRequestDataException(SERVICE_INFORMATION_NAME_REQUIRED_MESSAGE) }
        input?.let { ServiceSchemaValidator.validateSchema(it, "input").bind() }
        output?.let { ServiceSchemaValidator.validateSchema(it, "output").bind() }
    }

    fun checkInput(value: Any): Either<APIException, Unit> = either {
        input?.let { ServiceSchemaValidator.validateInput(it, value).bind() }
    }

    enum class ServiceMode(val key: String) {
        @JsonProperty("synchronous")
        SYNCHRONOUS("synchronous"),

        @JsonProperty("asynchronous")
        ASYNCHRONOUS("asynchronous");

        companion object {
            fun fromString(mode: String?): ServiceMode? =
                entries.firstOrNull { it.key == mode }
        }
    }
}
