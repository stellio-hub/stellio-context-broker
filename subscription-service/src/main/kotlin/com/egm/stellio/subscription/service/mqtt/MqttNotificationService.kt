package com.egm.stellio.subscription.service.mqtt

import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.subscription.model.Notification
import com.egm.stellio.subscription.model.Subscription
import org.eclipse.paho.client.mqttv3.MqttClient
import org.eclipse.paho.client.mqttv3.MqttConnectOptions
import org.eclipse.paho.client.mqttv3.MqttMessage
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.eclipse.paho.mqttv5.client.IMqttToken
import org.eclipse.paho.mqttv5.client.MqttAsyncClient
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import org.eclipse.paho.client.mqttv3.MqttException as MqttExceptionV3
import org.eclipse.paho.mqttv5.common.MqttException as MqttExceptionV5

@Service
class MqttNotificationService(
    @Value("\${subscription.mqtt.clientId}")
    private val clientId: String,
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    suspend fun notify(
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

        val brokerPort = if (uri.port != -1) uri.port else Mqtt.SCHEME.defaultPortMap[uri.scheme]

        val brokerUrl = "$brokerScheme://${uri.host}:$brokerPort"
        val notifierInfo = endpoint.notifierInfo?.associate { it.key to it.value } ?: emptyMap()
        val qos =
            notifierInfo[Mqtt.QualityOfService.KEY]?.let { Integer.parseInt(it) } ?: Mqtt.QualityOfService.AT_MOST_ONCE

        val data = MqttNotificationData(
            topic = uri.path,
            qos = qos,
            message = MqttNotificationData.MqttMessage(notification, headers),
            connection = MqttConnectionData(
                brokerUrl = brokerUrl,
                clientId = clientId,
                username = username,
                password = password
            )
        )

        try {
            val mqttVersion = notifierInfo[Mqtt.Version.KEY]
            when (mqttVersion) {
                Mqtt.Version.V3 -> callMqttV3(data)
                Mqtt.Version.V5 -> callMqttV5(data)
                else -> callMqttV5(data)
            }
            logger.info("successfull mqtt notification for uri : $uri version: $mqttVersion")
            return true
        } catch (e: MqttExceptionV3) {
            logger.error("failed mqttv3 notification for uri : $uri", e)
            return false
        } catch (e: MqttExceptionV5) {
            logger.error("failed mqttv5 notification for uri : $uri", e)
            return false
        }
    }

    internal suspend fun callMqttV3(data: MqttNotificationData) {
        val mqttClient = connectMqttv3(data.connection)
        val message = MqttMessage(
            serializeObject(data.message).toByteArray()
        )
        message.qos = data.qos
        mqttClient.publish(data.topic, message)
        mqttClient.disconnect()
    }

    internal suspend fun connectMqttv3(data: MqttConnectionData): MqttClient {
        val persistence = MemoryPersistence()
        val mqttClient = MqttClient(data.brokerUrl, data.clientId, persistence)
        val connOpts = MqttConnectOptions().apply {
            isCleanSession = true
            userName = data.username
            password = data.password?.toCharArray() ?: "".toCharArray()
        }

        mqttClient.connect(connOpts)
        return mqttClient
    }

    internal suspend fun callMqttV5(data: MqttNotificationData) {
        val mqttClient = connectMqttv5(data.connection)
        val message = org.eclipse.paho.mqttv5.common.MqttMessage(serializeObject(data.message).toByteArray())
        message.qos = data.qos
        val token = mqttClient.publish(data.topic, message)
        token.waitForCompletion()
        mqttClient.disconnect()
        mqttClient.close()
    }

    internal suspend fun connectMqttv5(data: MqttConnectionData): MqttAsyncClient {
        val persistence = org.eclipse.paho.mqttv5.client.persist.MemoryPersistence()
        val mqttClient = MqttAsyncClient(data.brokerUrl, data.clientId, persistence)
        val connOpts = MqttConnectionOptions()
        connOpts.isCleanStart = true
        connOpts.userName = data.username
        connOpts.password = data.password?.toByteArray()
        val token: IMqttToken = mqttClient.connect(connOpts)
        token.waitForCompletion()
        return mqttClient
    }
}
