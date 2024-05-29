package com.egm.stellio.subscription.service.mqtt

import com.egm.stellio.shared.util.JsonUtils.serializeObject
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions
import org.springframework.stereotype.Service
import org.eclipse.paho.client.mqttv3.MqttClient as MqttClientv3
import org.eclipse.paho.client.mqttv3.MqttConnectOptions as MqttConnectOptionsv3
import org.eclipse.paho.client.mqttv3.MqttMessage as MqttMessagev3
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence as MemoryPersistencev3
import org.eclipse.paho.mqttv5.client.IMqttToken as IMqttTokenv5
import org.eclipse.paho.mqttv5.client.MqttAsyncClient as MqttAsyncClientv5
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence as MemoryPersistencev5
import org.eclipse.paho.mqttv5.common.MqttMessage as MqttMessagev5

@Service
class MQTTVersionService {

    internal suspend fun callMqttV3(data: MQTTNotificationData) {
        val persistence = MemoryPersistencev3()
        val sampleClient = MqttClientv3(data.brokerUrl, data.clientId, persistence)
        val connOpts = MqttConnectOptionsv3()
        connOpts.isCleanSession = true
        connOpts.userName = data.username
        connOpts.password = data.password?.toCharArray() ?: "".toCharArray()
        sampleClient.connect(connOpts)
        val message = MqttMessagev3(
            serializeObject(data.mqttMessage).toByteArray()
        )
        message.qos = data.qos
        sampleClient.publish(data.topic, message)
        sampleClient.disconnect()
    }

    internal suspend fun callMqttV5(data: MQTTNotificationData) {
        val persistence = MemoryPersistencev5()
        val sampleClient = MqttAsyncClientv5(data.brokerUrl, data.clientId, persistence)
        val connOpts = MqttConnectionOptions()
        connOpts.isCleanStart = true
        connOpts.userName = data.username
        connOpts.password = data.password?.toByteArray()
        var token: IMqttTokenv5 = sampleClient.connect(connOpts)
        token.waitForCompletion()
        val message = MqttMessagev5(serializeObject(data.mqttMessage).toByteArray())
        message.qos = data.qos
        token = sampleClient.publish(data.topic, message)
        token.waitForCompletion()
        sampleClient.disconnect()
        sampleClient.close()
    }
}
