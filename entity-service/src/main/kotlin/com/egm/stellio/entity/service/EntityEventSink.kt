package com.egm.stellio.entity.service

import org.springframework.cloud.stream.annotation.Input
import org.springframework.messaging.SubscribableChannel

interface EntityEventSink {

    @Input("cim.entities")
    fun input(): SubscribableChannel
}
