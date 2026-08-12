package com.egm.stellio.search.service.execution.web

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.raise.ensure
import arrow.core.right
import com.egm.stellio.search.authorization.permission.service.AuthorizationService
import com.egm.stellio.search.service.execution.model.ServiceExecution.Companion.deserialize
import com.egm.stellio.search.service.execution.service.ServiceExecutionLauncher
import com.egm.stellio.search.service.execution.service.ServiceExecutionService
import com.egm.stellio.search.service.registration.model.ServiceInformation.ServiceMode
import com.egm.stellio.search.service.registration.service.ServiceRegistrationService
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.InvalidRequestException
import com.egm.stellio.shared.model.JSONLD_CONTEXT_KW
import com.egm.stellio.shared.queryparameter.AllowedParameters
import com.egm.stellio.shared.queryparameter.OptionsValue
import com.egm.stellio.shared.queryparameter.QP
import com.egm.stellio.shared.util.ErrorMessages.ServiceExecutionErrorMessages.SERVICE_EXECUTION_COMPLETION_CREATE_MESSAGE
import com.egm.stellio.shared.util.ErrorMessages.ServiceExecutionErrorMessages.SERVICE_EXECUTION_DELETE_OPTIONS_MESSAGE
import com.egm.stellio.shared.util.ErrorMessages.ServiceExecutionErrorMessages.SERVICE_EXECUTION_UPDATE_MEMBERS_MESSAGE
import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.JSON_MERGE_PATCH_CONTENT_TYPE
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.buildContextLinkHeader
import com.egm.stellio.shared.util.checkAndGetContext
import com.egm.stellio.shared.util.getApplicableMediaType
import com.egm.stellio.shared.util.getContextFromLinkHeaderOrDefault
import com.egm.stellio.shared.util.prepareGetSuccessResponseHeaders
import com.egm.stellio.shared.web.BaseHandler
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap
import org.springframework.validation.annotation.Validated
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PatchMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono
import java.net.URI

@RestController
@RequestMapping("/ngsi-ld/v1/services")
@Validated
class ServiceExecutionHandler(
    private val applicationProperties: ApplicationProperties,
    private val serviceExecutionService: ServiceExecutionService,
    private val serviceRegistrationService: ServiceRegistrationService,
    private val serviceExecutionLauncher: ServiceExecutionLauncher,
    private val authorizationService: AuthorizationService
) : BaseHandler() {

    @PostMapping(
        consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE],
        produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE]
    )
    suspend fun create(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>,
        @AllowedParameters
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        authorizationService.userIsAdmin().bind()
        val body = requestBody.awaitFirst().deserializeAsMap()
        val contexts = checkAndGetContext(httpHeaders, body, applicationProperties.contexts.core).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()
        val serviceExecution = deserialize(body, contexts).bind()
        ensure(serviceExecution.completion == null) {
            BadRequestDataException(SERVICE_EXECUTION_COMPLETION_CREATE_MESSAGE)
        }

        val serviceRegistration = serviceRegistrationService.getById(serviceExecution.serviceId).bind()
        serviceRegistration.serviceInformation.input?.checkValue(serviceExecution.input)?.bind()

        serviceExecutionService.create(serviceExecution).bind()
        val launchedExecution = if (serviceRegistration.serviceInformation.mode == ServiceMode.ASYNCHRONOUS) {
            serviceExecutionLauncher.invokeAsynchronousService(serviceExecution, serviceRegistration)
        } else {
            serviceExecutionLauncher.invoke(serviceExecution, serviceRegistration)
        }
        serviceExecutionService.upsert(launchedExecution).bind()

        ResponseEntity.status(HttpStatus.CREATED)
            .location(URI("/ngsi-ld/v1/services/${serviceExecution.id}"))
            .contentType(mediaType)
            .apply {
                if (mediaType != JSON_LD_MEDIA_TYPE)
                    header(HttpHeaders.LINK, buildContextLinkHeader(contexts.first()))
            }
            .body(launchedExecution.serialize(contexts, mediaType))
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @GetMapping("/{serviceExecutionId}", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun retrieve(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable serviceExecutionId: URI,
        @AllowedParameters(implemented = [QP.OPTIONS])
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        authorizationService.userIsAdmin().bind()
        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()
        val includeSysAttrs = queryParams.getFirst(QP.OPTIONS.key)
            ?.contains(OptionsValue.SYS_ATTRS.value) ?: false
        val serviceExecution = serviceExecutionService.getById(serviceExecutionId).bind()

        prepareGetSuccessResponseHeaders(mediaType, contexts)
            .body(serviceExecution.serialize(contexts, mediaType, includeSysAttrs))
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @PatchMapping(
        "/{serviceExecutionId}",
        consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE, JSON_MERGE_PATCH_CONTENT_TYPE]
    )
    suspend fun update(
        @PathVariable serviceExecutionId: URI,
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>,
        @AllowedParameters
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        authorizationService.userIsAdmin().bind()
        val currentExecution = serviceExecutionService.getById(serviceExecutionId).bind()
        val body = requestBody.awaitFirst().deserializeAsMap()
        val contexts = checkAndGetContext(httpHeaders, body, applicationProperties.contexts.core).bind()
        ensure((body.keys - JSONLD_CONTEXT_KW).all(PATCHABLE_MEMBERS::contains)) {
            BadRequestDataException(SERVICE_EXECUTION_UPDATE_MEMBERS_MESSAGE)
        }

        val updatedExecution = currentExecution.mergeWithFragment(body, contexts).bind()

        serviceExecutionService.upsert(updatedExecution).bind()

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @DeleteMapping("/{serviceExecutionId}")
    suspend fun delete(
        @PathVariable serviceExecutionId: URI,
        @AllowedParameters(implemented = [QP.OPTIONS])
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        authorizationService.userIsAdmin().bind()
        val options = (queryParams.getFirst(QP.OPTIONS.key) ?: DeleteOption.CANCEL.value)
            .split(",")
            .map { DeleteOption.fromString(it).bind() }
        if (DeleteOption.CANCEL in options)
            serviceExecutionLauncher.cancelExecution(serviceExecutionId).bind()
        if (DeleteOption.REMOVE in options)
            serviceExecutionService.delete(serviceExecutionId).bind()

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )
    companion object {
        private val PATCHABLE_MEMBERS = setOf("completion", "output", "executionStatus")

        private enum class DeleteOption(val value: String) {
            REMOVE("remove"),
            CANCEL("cancel");

            companion object {
                fun fromString(option: String): Either<APIException, DeleteOption> =
                    entries.find { it.value == option }?.right()
                        ?: InvalidRequestException(SERVICE_EXECUTION_DELETE_OPTIONS_MESSAGE).left()
            }
        }
    }
}
