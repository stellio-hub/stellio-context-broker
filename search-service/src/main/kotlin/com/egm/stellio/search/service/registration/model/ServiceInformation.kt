package com.egm.stellio.search.service.registration.model

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.ErrorMessages.ServiceRegistration.SERVICE_INFORMATION_NAME_REQUIRED_MESSAGE
import com.fasterxml.jackson.annotation.JsonProperty

data class ServiceInformation(
    val name: String = "",
    val title: String? = null,
    val description: String? = null,
    val mode: ServiceMode? = null,
    val input: InputInformation? = null,
    val output: InputInformation? = null
) {
    fun validate(): Either<APIException, Unit> =
        if (name.isBlank())
            BadRequestDataException(SERVICE_INFORMATION_NAME_REQUIRED_MESSAGE).left()
        else Unit.right()

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
