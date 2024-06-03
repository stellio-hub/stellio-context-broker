package com.egm.stellio.subscription.service.mqtt

import com.egm.stellio.subscription.model.Notification

data class MqttNotificationData(
    val topic: String,
    val mqttMessage: MqttMessage,
    val qos: Int,
    val brokerUrl: String,
    val clientId: String,
    val username: String,
    val password: String? = null,
) {

    data class MqttMessage(
        val body: Notification,
        val metadata: Map<String, String> = emptyMap(),
    )
}
