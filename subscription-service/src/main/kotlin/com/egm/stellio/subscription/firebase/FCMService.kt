package com.egm.stellio.subscription.firebase

import com.google.firebase.messaging.FirebaseMessaging
import com.google.firebase.messaging.Message
import com.google.firebase.messaging.Notification
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class FCMService {

    private val logger = LoggerFactory.getLogger(javaClass)

    fun sendMessage(notificationData: Map<String, String?>, messageData: Map<String, String>, deviceToken: String): String? {
        return try {
            sendAndGetResponse(
                createMessage(messageData, createNotification(notificationData), deviceToken)
            )
        } catch (e: Exception) {
            logger.warn(e.message)
            null
        }
    }

    private fun sendAndGetResponse(message: Message): String {
        return FirebaseMessaging.getInstance().sendAsync(message).get()
    }

    private fun createMessage(messageData: Map<String, String>, notification: Notification, deviceToken: String): Message {
        val message = Message.builder()
        messageData.forEach {
            message.putData(it.key, it.value)
        }
        return message.setNotification(notification).setToken(deviceToken).build()
    }

    private fun createNotification(notificationData: Map<String, String?>): Notification {
        return Notification.builder()
            .setTitle(notificationData["title"])
            .setBody(notificationData["body"])
            .build()
    }
}