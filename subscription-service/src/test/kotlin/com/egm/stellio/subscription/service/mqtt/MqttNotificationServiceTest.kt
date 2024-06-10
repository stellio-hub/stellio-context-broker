package com.egm.stellio.subscription.service.mqtt

import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SUBSCRIPTION_TERM
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.subscription.model.*
import com.egm.stellio.subscription.support.WithMosquittoContainer
import com.ninjasquad.springmockk.SpykBean
import io.mockk.coEvery
import io.mockk.coVerify
import kotlinx.coroutines.test.runTest
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken
import org.eclipse.paho.client.mqttv3.MqttException
import org.eclipse.paho.mqttv5.client.IMqttToken
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse
import org.eclipse.paho.mqttv5.common.MqttSubscription
import org.eclipse.paho.mqttv5.common.packet.MqttProperties
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.net.URI
import org.eclipse.paho.client.mqttv3.MqttCallback as MqttCallbackV3
import org.eclipse.paho.client.mqttv3.MqttMessage as MqttMessageV3
import org.eclipse.paho.mqttv5.client.MqttCallback as MqttCallbackV5
import org.eclipse.paho.mqttv5.common.MqttMessage as MqttMessageV5

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [MqttNotificationService::class])
@ActiveProfiles("test")
class MqttNotificationServiceTest : WithMosquittoContainer {

    @SpykBean
    private lateinit var mqttNotificationService: MqttNotificationService

    private val mqttContainerPort = WithMosquittoContainer.getBasicPort()

    private val mqttSubscriptionV3 = Subscription(
        type = NGSILD_SUBSCRIPTION_TERM,
        subscriptionName = "My Subscription",
        description = "My beautiful subscription",
        entities = emptySet(),
        notification = NotificationParams(
            attributes = emptyList(),
            endpoint = Endpoint(
                uri = "mqtt://test@localhost:$mqttContainerPort/notification".toUri(),
                accept = Endpoint.AcceptType.JSONLD,
                notifierInfo = listOf(
                    EndpointInfo(Mqtt.Version.KEY, Mqtt.Version.V3),
                    EndpointInfo(Mqtt.QualityOfService.KEY, Mqtt.QualityOfService.EXACTLY_ONCE.toString())
                )
            )
        ),
        contexts = emptyList()
    )

    private val mqttSubscriptionV5 = Subscription(
        type = NGSILD_SUBSCRIPTION_TERM,
        entities = emptySet(),
        notification = NotificationParams(
            attributes = emptyList(),
            endpoint = Endpoint(
                uri = "mqtt://test@localhost:$mqttContainerPort/notification".toUri(),
                notifierInfo = listOf(
                    EndpointInfo(Mqtt.Version.KEY, Mqtt.Version.V5),
                    EndpointInfo(Mqtt.QualityOfService.KEY, Mqtt.QualityOfService.EXACTLY_ONCE.toString())
                )
            )
        ),
        contexts = emptyList()
    )
    private val validMqttNotificationData = MqttNotificationData(
        connection = MqttConnectionData(
            brokerUrl = "tcp://localhost:$mqttContainerPort",
            clientId = "clientId",
            username = "test",
        ),
        topic = "/notification",
        qos = 0,
        message = MqttNotificationData.MqttMessage(getNotificationForSubscription(mqttSubscriptionV3), emptyMap())
    )

    private val notification = Notification(
        subscriptionId = URI("1"),
        data = listOf(mapOf("hello" to "world"))
    )

    private val invalidUriMqttNotificationData = MqttNotificationData(
        connection = MqttConnectionData(
            brokerUrl = "tcp://badHost:1883",
            clientId = "clientId",
            username = "test",
        ),
        topic = "notification",
        qos = 0,
        message = MqttNotificationData.MqttMessage(
            notification,
            emptyMap(),
        )
    )

    private fun getNotificationForSubscription(subscription: Subscription) = Notification(
        subscriptionId = subscription.id,
        data = listOf(mapOf("hello" to "world"))
    )

    @Test
    fun `mqttNotifier should process endpoint uri to get connection information`() = runTest {
        val subscription = mqttSubscriptionV3
        coEvery { mqttNotificationService.callMqttV3(any()) } returns Unit
        assert(
            mqttNotificationService.mqttNotifier(
                subscription,
                getNotificationForSubscription(subscription),
                mapOf()
            )
        )

        coVerify {
            mqttNotificationService.callMqttV3(
                match {
                    it.connection.username == validMqttNotificationData.connection.username &&
                        it.connection.password == validMqttNotificationData.connection.password &&
                        it.topic == validMqttNotificationData.topic &&
                        it.connection.brokerUrl == validMqttNotificationData.connection.brokerUrl
                }
            )
        }
    }

    @Test
    fun `mqttNotifier should use notifier info to choose the mqtt version`() = runTest {
        val subscription = mqttSubscriptionV3
        coEvery { mqttNotificationService.callMqttV3(any()) } returns Unit
        coEvery { mqttNotificationService.callMqttV5(any()) } returns Unit

        mqttNotificationService.mqttNotifier(
            subscription,
            getNotificationForSubscription(subscription),
            mapOf()
        )
        coVerify(exactly = 1) {
            mqttNotificationService.callMqttV3(
                any()
            )
        }
        coVerify(exactly = 0) {
            mqttNotificationService.callMqttV5(
                any()
            )
        }

        mqttNotificationService.mqttNotifier(
            mqttSubscriptionV5,
            getNotificationForSubscription(subscription),
            mapOf()
        )
        coVerify(exactly = 1) {
            mqttNotificationService.callMqttV5(
                any()
            )
        }
        coVerify(exactly = 1) {
            mqttNotificationService.callMqttV3(
                any()
            )
        }
    }

    @Test
    fun `sending mqttV3 notification with good uri should succeed`() = runTest {
        // if we give the same clientId the mqtt server close the connection
        val testConnectionData = validMqttNotificationData.connection.copy(clientId = "test-broker")
        val mqttClient = mqttNotificationService.connectMqttv3(testConnectionData)
        val messageReceiver = MqttV3MessageReceiver()
        mqttClient.setCallback(messageReceiver)
        mqttClient.subscribe(validMqttNotificationData.topic)

        assertDoesNotThrow { mqttNotificationService.callMqttV3(validMqttNotificationData) }
        Thread.sleep(20) // wait to receive notification in message receiver
        assertEquals(
            serializeObject(validMqttNotificationData.message),
            messageReceiver.lastReceivedMessage
        )
        mqttClient.disconnect()
        mqttClient.close()
    }

    @Test
    fun `sending mqttV3 notification with bad uri should throw an exception`() = runTest {
        assertThrows<MqttException> { mqttNotificationService.callMqttV3(invalidUriMqttNotificationData) }
    }

    @Test
    fun `sending mqttV5 notification with good uri should succeed`() = runTest {
        val testConnectionData = validMqttNotificationData.connection.copy(clientId = "test-broker")
        val mqttClient = mqttNotificationService.connectMqttv5(testConnectionData)
        val messageReceiver = MqttV5MessageReceiver()
        mqttClient.setCallback(messageReceiver)
        mqttClient.subscribe(MqttSubscription(validMqttNotificationData.topic))

        assertDoesNotThrow { mqttNotificationService.callMqttV5(validMqttNotificationData) }

        assertEquals(
            serializeObject(validMqttNotificationData.message),
            messageReceiver.lastReceivedMessage
        )
        mqttClient.disconnect()
        mqttClient.close()
    }

    @Test
    fun `sending mqttV5 notification with bad uri should throw an exception`() = runTest {
        assertThrows<org.eclipse.paho.mqttv5.common.MqttException> {
            mqttNotificationService.callMqttV5(
                invalidUriMqttNotificationData
            )
        }
    }

    private class MqttV3MessageReceiver : MqttCallbackV3 {
        var lastReceivedMessage: String? = null

        override fun messageArrived(topic: String, message: MqttMessageV3) {
            lastReceivedMessage = message.payload?.decodeToString()
        }

        override fun deliveryComplete(p0: IMqttDeliveryToken?) {
            println("delivery complete")
        }

        override fun connectionLost(p0: Throwable?) {
            println("connection lost")
        }
    }

    private class MqttV5MessageReceiver : MqttCallbackV5 {
        var lastReceivedMessage: String? = null

        override fun messageArrived(topic: String?, message: MqttMessageV5?) {
            lastReceivedMessage = message?.payload?.decodeToString()
        }

        override fun disconnected(p0: MqttDisconnectResponse?) {
            println("connection lost")
        }

        override fun mqttErrorOccurred(p0: org.eclipse.paho.mqttv5.common.MqttException?) {
            println("mqtt error occured")
        }

        override fun deliveryComplete(p0: IMqttToken?) {
            println("delivery complete")
        }

        override fun connectComplete(p0: Boolean, p1: String?) {
            println("connection success")
        }

        override fun authPacketArrived(p0: Int, p1: MqttProperties?) {
            println("auth")
        }
    }
}
