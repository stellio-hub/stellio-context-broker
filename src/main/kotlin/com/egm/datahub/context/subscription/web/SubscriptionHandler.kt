package com.egm.datahub.context.subscription.web

import com.egm.datahub.context.subscription.service.SubscriptionService
import com.egm.datahub.context.subscription.utils.NgsiLdParsingUtils
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.*
import org.springframework.web.reactive.function.server.bodyToMono
import reactor.core.publisher.Mono
import java.lang.reflect.UndeclaredThrowableException
import java.net.URI

@Component
class SubscriptionHandler(
    private val subscriptionService: SubscriptionService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * Implements 6.10.3.1 - Create Subscription
     */
    fun create(req: ServerRequest): Mono<ServerResponse> {

        return req.bodyToMono<String>()
                .map {
                    val context = NgsiLdParsingUtils.getContextOrThrowError(it)
                    NgsiLdParsingUtils.parseSubscription(it, context)
                }
                .flatMap { subscription ->
                    subscriptionService.exists(subscription).flatMap {
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
                        is AlreadyExistsException -> status(HttpStatus.CONFLICT).body(BodyInserters.fromValue(it.message.toString()))
                        is InternalErrorException -> status(HttpStatus.INTERNAL_SERVER_ERROR).build()
                        is BadRequestDataException -> status(HttpStatus.BAD_REQUEST).body(BodyInserters.fromValue(it.message.toString()))
                        is UndeclaredThrowableException -> badRequest().body(BodyInserters.fromValue(it.message.toString()))
                        else -> badRequest().body(BodyInserters.fromValue(it.message.toString()))
                    }
                }
    }
}