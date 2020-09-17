package com.egm.stellio.shared.util

import org.slf4j.LoggerFactory
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

object HttpUtils {

    private val logger = LoggerFactory.getLogger(javaClass)

    private val httpClient: HttpClient = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .build()

    fun doGet(uri: String): String? {
        val request = HttpRequest.newBuilder().GET().uri(uri.toUri()).build()
        return try {
            val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
            response.body()
        } catch (e: Exception) {
            logger.warn(e.message ?: "Error encountered while processing GET request")
            null
        }
    }
}
