package com.egm.stellio.shared.util

import com.egm.stellio.shared.util.OptionsParamValue.TEMPORAL_VALUES
import com.egm.stellio.shared.web.CustomWebFilter
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import java.util.Optional

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [ApiUtils::class])
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
    fun `it should support Mime-type with parameters`() {
        webClient.post()
            .uri("/router/mockkedroute")
            .header(HttpHeaders.CONTENT_TYPE, "application/ld+json;charset=UTF-8")
            .bodyValue("Some body")
            .exchange()
            .expectStatus().isCreated
    }
}
