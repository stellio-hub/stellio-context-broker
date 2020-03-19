package com.egm.stellio.subscription.web

import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.InternalErrorException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.NgsiLdParsingUtils
import com.egm.stellio.subscription.utils.parseSubscription
import com.egm.stellio.subscription.utils.parseSubscriptionUpdate
import com.egm.stellio.subscription.service.SubscriptionService
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.*
import org.springframework.web.reactive.function.server.bodyToMono
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono
import java.lang.reflect.UndeclaredThrowableException
import java.net.URI

@Component
class SubscriptionHandler(
    private val subscriptionService: SubscriptionService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    fun generatesProblemDetails(list: List<String>): String {
        val mapper = jacksonObjectMapper()
        return mapper.writeValueAsString(mapOf("ProblemDetails" to list))
    }

    /**
     * Implements 6.10.3.1 - Create Subscription
     */
    fun create(req: ServerRequest): Mono<ServerResponse> {

        return req.bodyToMono<String>()
                .map {
                    val context = NgsiLdParsingUtils.getContextOrThrowError(it)
                    parseSubscription(it, context)
                }
                .flatMap { subscription ->
                    subscriptionService.exists(subscription.id).flatMap {
                        Mono.just(Pair(subscription, it))
                    }
                }
                .map {
                    if (it.second)
                        throw AlreadyExistsException("A subscription with id ${it.first.id} already exists")
                    else {
                        subscriptionService.create(it.first).subscribe()
                        it.first
                    }
                }
                .flatMap {
                    created(URI("/ngsi-ld/v1/subscriptions/${it.id}")).build()
                }.onErrorResume {
                    when (it) {
                        is AlreadyExistsException -> status(HttpStatus.CONFLICT).body(BodyInserters.fromValue(generatesProblemDetails(listOf(it.message.toString()))))
                        is InternalErrorException -> status(HttpStatus.INTERNAL_SERVER_ERROR).build()
                        is BadRequestDataException -> status(HttpStatus.BAD_REQUEST).body(BodyInserters.fromValue(it.message.toString()))
                        is UndeclaredThrowableException -> badRequest().body(BodyInserters.fromValue(generatesProblemDetails(listOf(it.message.toString()))))
                        else -> badRequest().body(BodyInserters.fromValue(generatesProblemDetails(listOf(it.message.toString()))))
                    }
                }
    }

    /**
     * Implements 6.11.3.1 - Retrieve Subscription
     */
    fun getByURI(req: ServerRequest): Mono<ServerResponse> {
        val uri = req.pathVariable("subscriptionId")
        return uri.toMono()
                .flatMap {
                    subscriptionService.getById(uri)
                }
                .flatMap {
                    ok().body(BodyInserters.fromValue(it))
                }
                .onErrorResume {
                    when (it) {
                        is ResourceNotFoundException -> status(HttpStatus.NOT_FOUND).build()
                        else -> badRequest().body(BodyInserters.fromValue(generatesProblemDetails(listOf(it.message.toString()))))
                    }
                }
    }

    /**
     * Implements 6.11.3.2 - Update Subscription
     */
    fun update(req: ServerRequest): Mono<ServerResponse> {
        val subscriptionId = req.pathVariable("subscriptionId")
        return req.bodyToMono<String>()
            .flatMap { input ->
                subscriptionService.exists(subscriptionId).flatMap {
                    Mono.just(Pair(input, it))
                }
            }
            .flatMap {
                if (!it.second)
                    throw ResourceNotFoundException("Could not find a subscription with id $subscriptionId")

                val parsedInput = parseSubscriptionUpdate(it.first)
                subscriptionService.update(subscriptionId, parsedInput)
            }
            .flatMap {
                noContent().build()
            }
            .onErrorResume {
                when (it) {
                    is ResourceNotFoundException -> status(HttpStatus.NOT_FOUND).build()
                    is BadRequestDataException -> status(HttpStatus.BAD_REQUEST).body(BodyInserters.fromValue(it.message.toString()))
                    else -> badRequest().body(BodyInserters.fromValue(generatesProblemDetails(listOf(it.message.toString()))))
                }
            }
    }

    /**
     * Implements 6.11.3.3 - Delete Subscription
     */
    fun delete(req: ServerRequest): Mono<ServerResponse> {
        val subscriptionId = req.pathVariable("subscriptionId")

        return subscriptionId.toMono()
                .flatMap {
                    subscriptionService.delete(subscriptionId)
                }
                .flatMap {
                    if (it >= 1)
                        noContent().build()
                    else
                        notFound().build()
                }
                .onErrorResume {
                    status(HttpStatus.INTERNAL_SERVER_ERROR).body(BodyInserters.fromValue(generatesProblemDetails(listOf(it.localizedMessage))))
                }
    }
}