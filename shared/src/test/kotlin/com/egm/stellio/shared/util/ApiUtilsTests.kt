package com.egm.stellio.shared.util

import com.egm.stellio.shared.web.CustomWebFilter
import com.egm.stellio.shared.util.OptionsParamValue.TEMPORAL_VALUES
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.test.context.ActiveProfiles
import java.util.*
import org.springframework.test.web.reactive.server.WebTestClient

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [ ApiUtils::class ])
@ActiveProfiles("test")
class ApiUtilsTests {
    private val webClient = WebTestClient.bindToController(MockkedHandler()).webFilter<WebTestClient.ControllerSpec>(
        CustomWebFilter()
    ).build()

    @Test
    fun `it should not find a value if there is no options query param`() {
        assertFalse(hasValueInOptionsParam(Optional.empty(), TEMPORAL_VALUES))
    }

    @Test
    fun `it should not find a value if it is not in a single value options query param`() {
        assertFalse(hasValueInOptionsParam(Optional.of("one"), TEMPORAL_VALUES))
    }

    @Test
    fun `it should not find a value if it is not in a multi value options query param`() {
        assertFalse(hasValueInOptionsParam(Optional.of("one,two"), TEMPORAL_VALUES))
    }

    @Test
    fun `it should find a value if it is in a single value options query param`() {
        assertTrue(hasValueInOptionsParam(Optional.of("temporalValues"), TEMPORAL_VALUES))
    }

    @Test
    fun `it should find a value if it is in a multi value options query param`() {
        assertTrue(hasValueInOptionsParam(Optional.of("one,temporalValues"), TEMPORAL_VALUES))
    }

    @Test
    fun `it should return 411 if Content-Length is null`() {

        webClient.post()
            .uri("/router/mockkedroute")
            .header(HttpHeaders.CONTENT_TYPE, "application/json")
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.LENGTH_REQUIRED)
            .expectBody().isEmpty
    }

    @Test
    fun `it should return 415 if Content-Type is not supported`() {

        webClient.post()
            .uri("/router/mockkedroute")
            .header(HttpHeaders.CONTENT_LENGTH, "3495")
            .header(HttpHeaders.CONTENT_TYPE, "application/pdf")
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE)
            .expectBody().isEmpty
    }

    @Test
    fun `it should not accept merge-patch+json as Content-type for non patch requests`() {

        webClient.post()
            .uri("/router/mockkedroute")
            .header(HttpHeaders.CONTENT_LENGTH, "3495")
            .header(HttpHeaders.CONTENT_TYPE, "application/merge-patch+json")
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE)
    }

    @Test
    fun `it should accept merge-patch+json as Content-type for patch requests`() {

        webClient.patch()
            .uri("/router/mockkedroute")
            .header(HttpHeaders.CONTENT_LENGTH, "3495")
            .header(HttpHeaders.CONTENT_TYPE, "application/merge-patch+json")
            .exchange()
            .expectStatus().isNoContent
    }

    @Test
    fun `it should return 406 if accept header format is not acceptable`() {

        webClient.get()
            .uri("/router/mockkedroute")
            .header(HttpHeaders.CONTENT_LENGTH, "3495")
            .header(HttpHeaders.CONTENT_TYPE, "application/json")
            .header(HttpHeaders.ACCEPT, "unknown/*")
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.NOT_ACCEPTABLE)
    }

    @Test
    fun `it should accept application|json accept header`() {

        webClient.get()
            .uri("/router/mockkedroute")
            .header(HttpHeaders.CONTENT_LENGTH, "3495")
            .header(HttpHeaders.ACCEPT, "application/json")
            .exchange()
            .expectStatus().isOk
    }

    @Test
    fun `it should accept application|ld+json accept header`() {

        webClient.get()
            .uri("/router/mockkedroute")
            .header(HttpHeaders.CONTENT_LENGTH, "3495")
            .header(HttpHeaders.ACCEPT, "application/ld+json")
            .exchange()
            .expectStatus().isOk
    }

    @Test
    fun `it should accept application|* accept header`() {

        webClient.get()
            .uri("/router/mockkedroute")
            .header(HttpHeaders.CONTENT_LENGTH, "3495")
            .header(HttpHeaders.ACCEPT, "application/*")
            .exchange()
            .expectStatus().isOk
    }

    @Test
    fun `it should accept *|* accept header`() {

        webClient.get()
            .uri("/router/mockkedroute")
            .header(HttpHeaders.CONTENT_LENGTH, "3495")
            .header(HttpHeaders.ACCEPT, "*/*")
            .exchange()
            .expectStatus().isOk
    }
}