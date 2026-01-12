package com.egm.stellio.shared.util

inline fun <reified T> T.wrapToList(): List<T> = listOf(this)
