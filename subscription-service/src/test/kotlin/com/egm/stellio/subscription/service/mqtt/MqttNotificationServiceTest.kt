package com.egm.stellio.subscription.service.mqtt

import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SUBSCRIPTION_TERM
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.subscription.model.*
import com.egm.stellio.subscription.support.WithMosquittoContainer
import com.ninjasquad.springmockk.SpykBean
import io.mockk.coEvery
import io.mockk.coVerify
import kotlinx.coroutines.test.runTest
import org.eclipse.paho.client.mqttv3.MqttException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.net.URI

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [MqttNotificationService::class])
@ActiveProfiles("test")
class MqttNotificationServiceTest : WithMosquittoContainer {

    @SpykBean
    private lateinit var mqttNotificationService: MqttNotificationService

    private val mqttContainerPort = WithMosquittoContainer.mosquittoContainer.getMappedPort(
        Mqtt.SCHEME.MQTT_DEFAULT_PORT
    )
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
                    EndpointInfo(Mqtt.QualityOfService.KEY, Mqtt.QualityOfService.AT_MOST_ONCE.toString())
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
                    EndpointInfo(Mqtt.Version.KEY, Mqtt.Version.V5)
                )
            )
        ),
        contexts = emptyList()
    )
    private val validMqttNotificationData = MqttNotificationData(
        brokerUrl = "tcp://localhost:$mqttContainerPort",
        clientId = "clientId",
        username = "test",
        topic = "/notification",
        qos = 0,
        mqttMessage = MqttNotificationData.MqttMessage(getNotificationForSubscription(mqttSubscriptionV3), emptyMap())
    )

    private val notification = Notification(
        subscriptionId = URI("1"),
        data = listOf(mapOf("hello" to "world"))
    )

    private val invalidUriMqttNotificationData = MqttNotificationData(
        brokerUrl = "tcp://badHost:1883",
        clientId = "clientId",
        username = "test",
        topic = "notification",
        qos = 0,
        mqttMessage = MqttNotificationData.MqttMessage(notification, emptyMap())
    )

    private fun getNotificationForSubscription(subscription: Subscription) = Notification(
        subscriptionId = subscription.id,
        data = listOf(mapOf("hello" to "world"))
    )

    @Test
    fun `mqttNotifier should process endpoint uri to get connexion information`() = runTest {
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
                    it.username == validMqttNotificationData.username &&
                        it.password == validMqttNotificationData.password &&
                        it.topic == validMqttNotificationData.topic &&
                        it.brokerUrl == validMqttNotificationData.brokerUrl
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
        assertDoesNotThrow { mqttNotificationService.callMqttV3(validMqttNotificationData) }
    }

    @Test
    fun `sending mqttV3 notification with bad uri should throw an exception`() = runTest {
        assertThrows<MqttException> { mqttNotificationService.callMqttV3(invalidUriMqttNotificationData) }
    }

    @Test
    fun `sending mqttV5 notification with good uri should succeed`() = runTest {
        assertDoesNotThrow { mqttNotificationService.callMqttV5(validMqttNotificationData) }
    }

    @Test
    fun `sending mqttV5 notification with bad uri should throw an exception`() = runTest {
        assertThrows<org.eclipse.paho.mqttv5.common.MqttException> {
            mqttNotificationService.callMqttV5(
                invalidUriMqttNotificationData
            )
        }
    }
}
