package com.egm.stellio.shared.support

import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.ok
import com.github.tomakehurst.wiremock.client.WireMock.stubFor
import com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.options
import org.junit.jupiter.api.extension.AfterAllCallback
import org.junit.jupiter.api.extension.BeforeAllCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.slf4j.LoggerFactory

const val MOCK_SERVER_PORT = 8093

class JsonLdContextServerExtension : BeforeAllCallback, AfterAllCallback {

    private val logger = LoggerFactory.getLogger(javaClass)

    private lateinit var wireMockServer: WireMockServer

    override fun beforeAll(context: ExtensionContext) {
        wireMockServer = WireMockServer(
            options()
                .port(MOCK_SERVER_PORT)
                .usingFilesUnderClasspath("wiremock")
                .globalTemplating(true)
        )
        wireMockServer.start()
        WireMock.configureFor("localhost", MOCK_SERVER_PORT)
        stubFor(
            get(urlPathMatching("/jsonld-contexts/.*"))
                .willReturn(
                    ok()
                        .withHeader("Content-Type", JSON_LD_CONTENT_TYPE)
                        .withBodyFile("jsonld-contexts/{{request.pathSegments.[1]}}")
                )
        )

        logger.debug("JSON-LD context server is started")
    }

    override fun afterAll(context: ExtensionContext) {
        wireMockServer.stop()
    }
}
