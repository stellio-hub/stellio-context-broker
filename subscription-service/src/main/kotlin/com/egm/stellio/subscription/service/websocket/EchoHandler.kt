package com.egm.stellio.subscription.service.websocket

import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.WebSocketMessage
import org.springframework.web.reactive.socket.WebSocketSession
import reactor.core.publisher.Mono

/**
 * @author hantsy
 */
class EchoHandler : WebSocketHandler {
    override fun handle(session: WebSocketSession): Mono<Void> {
        return session.send(
            session.receive()
                .doOnNext { obj: WebSocketMessage -> obj.retain() } // Use retain() for Reactor Netty
                .map { m: WebSocketMessage ->
                    session.textMessage(
                        "received:" + m.payloadAsText
                    )
                }
        )
    }
}
