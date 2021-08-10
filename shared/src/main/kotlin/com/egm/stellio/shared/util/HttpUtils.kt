package com.egm.stellio.shared.util

import java.net.URLDecoder

fun String.decode(): String =
    URLDecoder.decode(this, "UTF-8")
