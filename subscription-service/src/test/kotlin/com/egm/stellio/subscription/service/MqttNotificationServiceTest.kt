package com.egm.stellio.subscription.service

import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SUBSCRIPTION_TERM
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.subscription.model.*
import com.egm.stellio.subscription.service.mqtt.MQTTNotificationData
import com.egm.stellio.subscription.service.mqtt.MQTTNotificationService
import com.egm.stellio.subscription.service.mqtt.MQTTVersionService
import com.egm.stellio.subscription.service.mqtt.Mqtt
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [MQTTNotificationService::class])
@ActiveProfiles("test")
class MqttNotificationServiceTest {

    @Autowired
    private lateinit var mqttNotificationService: MQTTNotificationService

    @MockkBean
    private lateinit var mqttVersionService: MQTTVersionService

    private val mqttSubscription = Subscription(
        type = NGSILD_SUBSCRIPTION_TERM,
        subscriptionName = "My Subscription",
        description = "My beautiful subscription",
        entities = emptySet(),
        notification = NotificationParams(
            attributes = emptyList(),
            endpoint = Endpoint(
                uri = "mqtt://test@localhost:1883/notification".toUri(),
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
                uri = "mqtt://test@localhost:1883/notification".toUri(),
                notifierInfo = listOf(
                    EndpointInfo(Mqtt.Version.KEY, Mqtt.Version.V5)
                )
            )
        ),
        contexts = emptyList()
    )
    private val validMqttNotificationData = MQTTNotificationData(
        brokerUrl = "tcp://localhost:1883",
        clientId = "clientId",
        username = "test",
        topic = "/notification",
        qos = 0,
        mqttMessage = MQTTNotificationData.MqttMessage(getNotificationForSubscription(mqttSubscription), emptyMap())
    )

    private fun getNotificationForSubscription(subscription: Subscription) = Notification(
        subscriptionId = subscription.id,
        data = listOf(mapOf("hello" to "world"))
    )

    @Test
    fun `mqttNotifier should process endpoint uri to get connexion information`() = runTest {
        val subscription = mqttSubscription
        coEvery { mqttVersionService.callMqttV3(any()) } returns Unit
        assert(
            mqttNotificationService.mqttNotifier(
                subscription,
                getNotificationForSubscription(subscription),
                mapOf()
            )
        )

        coVerify {
            mqttVersionService.callMqttV3(
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
        val subscription = mqttSubscription
        coEvery { mqttVersionService.callMqttV3(any()) } returns Unit
        coEvery { mqttVersionService.callMqttV5(any()) } returns Unit

        mqttNotificationService.mqttNotifier(
            subscription,
            getNotificationForSubscription(subscription),
            mapOf()
        )
        coVerify(exactly = 1) {
            mqttVersionService.callMqttV3(
                any()
            )
        }
        coVerify(exactly = 0) {
            mqttVersionService.callMqttV5(
                any()
            )
        }

        mqttNotificationService.mqttNotifier(
            mqttSubscriptionV5,
            getNotificationForSubscription(subscription),
            mapOf()
        )
        coVerify(exactly = 1) {
            mqttVersionService.callMqttV5(
                any()
            )
        }
        coVerify(exactly = 1) {
            mqttVersionService.callMqttV3(
                any()
            )
        }
    }
}
