package com.egm.stellio.subscription.service.mqtt

import com.egm.stellio.subscription.model.Endpoint.Companion.MQTTS_SCHEME
import com.egm.stellio.subscription.model.Endpoint.Companion.MQTT_SCHEME
import com.egm.stellio.subscription.service.mqtt.Mqtt.Port.MQTTS_DEFAULT_PORT
import com.egm.stellio.subscription.service.mqtt.Mqtt.Port.MQTT_DEFAULT_PORT

object Mqtt {

    object Version {
        const val KEY = "MQTT-Version"
        const val V3 = "mqtt3.1.1"
        const val V5 = "mqtt5.0"
    }

    object QualityOfService {
        const val KEY = "MQTT-QoS"
        const val AT_MOST_ONCE = 0
        const val EXACTLY_ONCE = 2
    }

    object Port {
        const val MQTT_DEFAULT_PORT = 1883
        const val MQTTS_DEFAULT_PORT = 8883
    }

    object SchemeMapping {
        val defaultPortMap = mapOf(MQTT_SCHEME to MQTT_DEFAULT_PORT, MQTTS_SCHEME to MQTTS_DEFAULT_PORT)
        val brokerSchemeMap = mapOf(MQTT_SCHEME to "tcp", MQTTS_SCHEME to "ssl")
    }
}
