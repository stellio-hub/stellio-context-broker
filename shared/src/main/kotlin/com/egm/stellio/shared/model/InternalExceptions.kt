package com.egm.stellio.shared.model

sealed class InternalException(
    override val message: String
) : Exception(message)

data class UnsupportedEventTypeException(override val message: String) : InternalException(message)
