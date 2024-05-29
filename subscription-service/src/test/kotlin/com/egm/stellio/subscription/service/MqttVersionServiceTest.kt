package com.egm.stellio.subscription.service

import com.egm.stellio.subscription.model.Notification
import com.egm.stellio.subscription.service.mqtt.MQTTNotificationData
import com.egm.stellio.subscription.service.mqtt.MQTTVersionService
import com.egm.stellio.subscription.service.mqtt.Mqtt
import com.egm.stellio.subscription.support.WithMosquittoContainer
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.net.URI
import org.eclipse.paho.client.mqttv3.MqttException as MqttExceptionV3
import org.eclipse.paho.mqttv5.common.MqttException as MqttExceptionV5

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [MQTTVersionService::class])
@ActiveProfiles("test")
class MqttVersionServiceTest : WithMosquittoContainer {

    @Autowired
    private lateinit var mqttVersionService: MQTTVersionService

    private val mqttContainerPort = WithMosquittoContainer.mosquittoContainer.getMappedPort(
        Mqtt.SCHEME.MQTT_DEFAULT_PORT
    )

    private val notification = Notification(
        subscriptionId = URI("1"),
        data = listOf(mapOf("hello" to "world"))
    )

    private val validMqttNotificationData = MQTTNotificationData(
        brokerUrl = "tcp://localhost:$mqttContainerPort",
        clientId = "clientId",
        username = "test",
        topic = "notification",
        qos = 0,
        mqttMessage = MQTTNotificationData.MqttMessage(notification, emptyMap())
    )

    private val invalidUriMqttNotificationData = MQTTNotificationData(
        brokerUrl = "tcp://badHost:1883",
        clientId = "clientId",
        username = "test",
        topic = "notification",
        qos = 0,
        mqttMessage = MQTTNotificationData.MqttMessage(notification, emptyMap())
    )

    @Test
    fun `sending mqttV3 notification with good uri should succeed`() = runTest {
        assertDoesNotThrow { mqttVersionService.callMqttV3(validMqttNotificationData) }
    }

    @Test
    fun `sending mqttV3 notification with bad uri should throw`() = runTest {
        assertThrows<MqttExceptionV3> { mqttVersionService.callMqttV3(invalidUriMqttNotificationData) }
    }

    @Test
    fun `sending mqttV5 notification with good uri should succeed`() = runTest {
        assertDoesNotThrow { mqttVersionService.callMqttV5(validMqttNotificationData) }
    }

    @Test
    fun `sending mqttV5 notification with bad uri should throw`() = runTest {
        assertThrows<MqttExceptionV5> { mqttVersionService.callMqttV5(invalidUriMqttNotificationData) }
    }
}
