package com.egm.datahub.context.search.listener

import org.springframework.cloud.stream.annotation.Input
import org.springframework.messaging.SubscribableChannel

interface ObservationsSink {

    @Input("cim.observations")
    fun input(): SubscribableChannel
}