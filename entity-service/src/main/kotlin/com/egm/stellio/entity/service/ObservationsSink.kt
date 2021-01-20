package com.egm.stellio.entity.service

import org.springframework.cloud.stream.annotation.Input
import org.springframework.messaging.SubscribableChannel

interface ObservationsSink {

    @Input("cim.deprecated.observations")
    fun input(): SubscribableChannel
}
