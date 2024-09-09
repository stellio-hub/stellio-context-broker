package com.egm.stellio.subscription.service.websocket

import com.egm.stellio.subscription.service.SubscriptionService
import org.springframework.messaging.handler.annotation.MessageMapping
import org.springframework.messaging.handler.annotation.SendTo
import org.springframework.stereotype.Controller
import java.text.SimpleDateFormat
import java.util.*

@Controller
class WSController(
    private val subscriptionService: SubscriptionService
) {

    @MessageMapping("/chat")
    @SendTo("/topic/messages")
    fun greeting(message: Message): OutputMessage {
        val time = SimpleDateFormat("HH:mm").format(Date())
        return OutputMessage(message.from!!, message.text!!, time)
    }
}

class Message {
    val from: String? = null
    val text: String? = null
}
