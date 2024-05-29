package com.egm.stellio.subscription.service.mqtt

import com.egm.stellio.shared.model.BadSchemeException
import com.egm.stellio.subscription.model.Notification
import com.egm.stellio.subscription.model.Subscription
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import org.eclipse.paho.client.mqttv3.MqttException as MqttExceptionV3
import org.eclipse.paho.mqttv5.common.MqttException as MqttExceptionV5

@Service
class MQTTNotificationService(
    @Value("\${mqtt.clientId}")
    private val clientId: String = "stellio-context-brokerUrl",
    private val mqttVersionService: MQTTVersionService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    suspend fun mqttNotifier(
        subscription: Subscription,
        notification: Notification,
        headers: Map<String, String>
    ): Boolean {
        val endpoint = subscription.notification.endpoint
        val uri = endpoint.uri
        val userInfo = uri.userInfo.split(':')
        val username = userInfo.getOrNull(0) ?: ""
        val password = userInfo.getOrNull(1)
        val brokerScheme = Mqtt.SCHEME.brokerSchemeMap[uri.scheme]
            ?: throw BadSchemeException("${uri.scheme} is not a valid mqtt scheme")

        val brokerPort = if (uri.port != -1) uri.port else Mqtt.SCHEME.defaultPortMap[uri.scheme]

        val brokerUrl = "$brokerScheme://${uri.host}:$brokerPort"
        val notifierInfo = endpoint.notifierInfo?.map { it.key to it.value }?.toMap() ?: emptyMap()
        val qos =
            notifierInfo[Mqtt.QualityOfService.KEY]?.let { Integer.parseInt(it) } ?: Mqtt.QualityOfService.AT_MOST_ONCE

        val data = MQTTNotificationData(
            topic = uri.path,
            brokerUrl = brokerUrl,
            clientId = clientId,
            qos = qos,
            mqttMessage = MQTTNotificationData.MqttMessage(notification, headers),
            username = username,
            password = password
        )

        try {
            val mqttVersion = notifierInfo[Mqtt.Version.KEY]
            when (mqttVersion) {
                Mqtt.Version.V3 -> mqttVersionService.callMqttV3(data)
                Mqtt.Version.V5 -> mqttVersionService.callMqttV5(data)
                else -> mqttVersionService.callMqttV5(data)
            }
            logger.info("successfull mqtt notification for uri : ${data.brokerUrl} version: $mqttVersion")
            return true
        } catch (e: MqttExceptionV3) {
            logger.error("failed mqttv3 notification for uri : ${data.brokerUrl}", e)
            return false
        } catch (e: MqttExceptionV5) {
            logger.error("failed mqttv5 notification for uri : ${data.brokerUrl}", e)
            return false
        }
    }
}
