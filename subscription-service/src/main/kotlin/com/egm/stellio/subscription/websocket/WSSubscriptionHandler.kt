package com.egm.stellio.subscription.websocket

import arrow.core.Option
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.subscription.model.Notification
import com.egm.stellio.subscription.model.Subscription
import com.egm.stellio.subscription.service.SubscriptionService
import com.egm.stellio.subscription.utils.ParsingUtils.parseSubscription
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.reactive.config.EnableWebFlux
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.WebSocketMessage
import org.springframework.web.reactive.socket.WebSocketSession
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.publisher.Sinks.Many

@Configuration
@EnableWebFlux
class WSSubscriptionHandler(
    val subscriptionService: SubscriptionService,
) : WebSocketHandler {

    private val logger = LoggerFactory.getLogger(javaClass)

    var sessionSinks: MutableMap<String, Many<Any>?> = mutableMapOf()

    @Bean
    fun simpleUrlHandlerMapping(): SimpleUrlHandlerMapping {
        val urlMap: MutableMap<String, WebSocketHandler?> = HashMap()
        urlMap.put("/echo", this::handle)

        val handlerMapping = SimpleUrlHandlerMapping()
        handlerMapping.urlMap = urlMap
        handlerMapping.order = 1

        return handlerMapping
    }

    override fun handle(session: WebSocketSession): Mono<Void> {
        val uniqueId = session.id
        logger.debug("Websocket connected for id $uniqueId")

        val sink: Many<Any> = Sinks.many().unicast().onBackpressureError()
        sink.asFlux().subscribe(WebSocketPublisher(session))
        sessionSinks[uniqueId] = sink

        val messageFlux = session.receive().share()
        val input = messageFlux.filter { webSocketMessage: WebSocketMessage ->
            webSocketMessage.type == WebSocketMessage.Type.TEXT
        }
            .map { obj: WebSocketMessage -> obj.payloadAsText }
            .doOnNext { message: String ->
                logger.debug("Received message from $uniqueId")
                val body = message.deserializeAsMap()
                val contexts = (body["metadata"] as Map<String, String>)["Link"] as String
                val subscription = parseSubscription(
                    body["body"] as Map<String, Any>,
                    listOf(contexts)
                ).getOrNull()!!

                val wsSubscription = subscription.copy(
                    notification = subscription.notification.copy(
                        endpoint = subscription.notification.endpoint.copy(
                            uri = "ws://$uniqueId".toUri()
                        )
                    )
                )
                runBlocking {
                    subscriptionService.create(
                        wsSubscription,
                        Option.fromNullable(null)
                    )
                }

                val emitResult = sink.tryEmitNext("received subscription: $message")
                logger.debug("Emit result status " + emitResult.name + " " + emitResult.isSuccess)
            }

        return Flux.merge(input).then()
    }

    fun notify(
        subscription: Subscription,
        notification: Notification,
        headers: Map<String, String>
    ): Boolean {
        val id = subscription.notification.endpoint.uri.host
        val sink = sessionSinks[id] ?: throw ClassNotFoundException("this websocket session doesn't exist")
        val notif = WSNotification(notification, headers)
        sink.tryEmitNext(serializeObject(notif))
        println("result send")
        return true
    }
}
