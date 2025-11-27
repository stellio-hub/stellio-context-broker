package com.egm.stellio.shared.util

import java.net.URLDecoder
import java.net.URLEncoder

object HttpUtils {
    fun String.decode(): String =
        URLDecoder.decode(this, "UTF-8")

    fun String.encode(): String =
        URLEncoder.encode(this, "UTF-8")
}
