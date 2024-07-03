package com.egm.stellio.subscription.service.mqtt

import com.egm.stellio.subscription.model.Notification

data class MqttNotificationData(
    val topic: String,
    val message: MqttMessage,
    val qos: Int,
    val connection: MqttConnectionData
) {

    data class MqttMessage(
        val body: Notification,
        val metadata: Map<String, String> = emptyMap(),
    )
}

data class MqttConnectionData(
    val brokerUrl: String,
    val clientId: String,
    val username: String? = null,
    val password: String? = null,
)
