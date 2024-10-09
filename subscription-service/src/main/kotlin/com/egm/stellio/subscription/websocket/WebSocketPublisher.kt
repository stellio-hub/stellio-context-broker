package com.egm.stellio.subscription.websocket

import org.springframework.web.reactive.socket.WebSocketSession
import reactor.core.publisher.Mono
import java.util.function.Consumer

class WebSocketPublisher(var session: WebSocketSession) : Consumer<Any?> {

    override fun accept(message: Any?) {
        session.send(Mono.just(session.textMessage(message as String))).subscribe()
    }
}
