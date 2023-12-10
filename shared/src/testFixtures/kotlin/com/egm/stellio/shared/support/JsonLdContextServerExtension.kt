package com.egm.stellio.shared.support

import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import org.junit.jupiter.api.extension.AfterAllCallback
import org.junit.jupiter.api.extension.BeforeAllCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.mockserver.integration.ClientAndServer
import org.mockserver.integration.ClientAndServer.startClientAndServer
import org.mockserver.mock.action.ExpectationResponseCallback
import org.mockserver.model.HttpClassCallback.callback
import org.mockserver.model.HttpRequest
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse
import org.mockserver.model.HttpResponse.notFoundResponse
import org.mockserver.model.HttpResponse.response
import org.mockserver.model.HttpStatusCode
import org.slf4j.LoggerFactory
import org.springframework.core.io.ClassPathResource

const val MOCK_SERVER_PORT = 8093

class JsonLdContextServerExtension : BeforeAllCallback, AfterAllCallback {

    private val logger = LoggerFactory.getLogger(javaClass)

    private lateinit var mockServer: ClientAndServer

    override fun beforeAll(context: ExtensionContext) {
        mockServer = startClientAndServer(MOCK_SERVER_PORT)

        mockServer
            .`when`(
                request().withMethod("GET").withPath("/jsonld-contexts/.*")
            )
            .respond(
                callback().withCallbackClass(JsonLdContextResponseCallback::class.java)
            )
        logger.debug("WireMock server is started")
    }

    override fun afterAll(context: ExtensionContext) {
        mockServer.stop()
    }

    class JsonLdContextResponseCallback : ExpectationResponseCallback {
        override fun handle(httpRequest: HttpRequest): HttpResponse {
            val contextFilename = httpRequest.path.value.substringAfterLast("/")
            val resource = ClassPathResource("/jsonld-contexts/$contextFilename")
            return if (resource.exists()) {
                response()
                    .withStatusCode(HttpStatusCode.OK_200.code())
                    .withHeader("Content-Type", JSON_LD_CONTENT_TYPE)
                    .withBody(resource.inputStream.readBytes())
            } else {
                notFoundResponse()
            }
        }
    }
}
