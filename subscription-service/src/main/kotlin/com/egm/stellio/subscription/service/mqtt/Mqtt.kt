package com.egm.stellio.subscription.service.mqtt

object Mqtt {

    object Version {
        const val KEY = "MQTT-Version"
        const val V3 = "mqtt3.1.1"
        const val V5 = "mqtt5.0"
    }

    object QualityOfService {
        const val KEY = "MQTT-QoS"
        const val AT_MOST_ONCE = 0
    }

    object SCHEME {
        const val MQTT = "mqtt"
        const val MQTTS = "mqtts"
        const val MQTT_DEFAULT_PORT = 1883
        const val MQTTS_DEFAULT_PORT = 8883
        val defaultPortMap = mapOf(MQTT to MQTT_DEFAULT_PORT, MQTTS to MQTTS_DEFAULT_PORT)
        val brokerSchemeMap = mapOf(MQTT to "tcp", MQTTS to "ssl")
    }
}
