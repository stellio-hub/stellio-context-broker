package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.OptionsParamValue.TEMPORAL_VALUES
import com.egm.stellio.shared.web.CustomWebFilter
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import java.util.Optional

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
    fun `it should return an empty list if no attrs param is provided`() {
        assertTrue(parseAndExpandRequestParameter(null, "").isEmpty())
    }

    @Test
    fun `it should return an singleton list if there is one provided attrs param`() {
        assertEquals(1, parseAndExpandRequestParameter("attr1", NGSILD_CORE_CONTEXT).size)
    }

    @Test
    fun `it should return a list with two elements if there are two provided attrs param`() {
        assertEquals(2, parseAndExpandRequestParameter("attr1, attr2", NGSILD_CORE_CONTEXT).size)
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

    @Test
    fun `it should get a single @context from a JSON-LD input`() {
        val httpHeaders = HttpHeaders().apply {
            set("Content-Type", "application/ld+json")
        }
        val jsonLdInput = mapOf(
            "id" to "urn:ngsi-ld:Building:farm001",
            "type" to "Building",
            "@context" to "https://fiware.github.io/data-models/context.jsonld"
        )

        val contexts = checkAndGetContext(httpHeaders, jsonLdInput)
        assertEquals(1, contexts.size)
        assertEquals("https://fiware.github.io/data-models/context.jsonld", contexts[0])
    }

    @Test
    fun `it should get a list of @context from a JSON-LD input`() {
        val httpHeaders = HttpHeaders().apply {
            set("Content-Type", "application/ld+json")
        }
        val jsonLdInput = mapOf(
            "id" to "urn:ngsi-ld:Building:farm001",
            "type" to "Building",
            "@context" to listOf(
                "https://fiware.github.io/data-models/context.jsonld",
                "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
            )
        )

        val contexts = checkAndGetContext(httpHeaders, jsonLdInput)
        assertEquals(2, contexts.size)
        assertTrue(
            contexts.containsAll(
                listOf(
                    "https://fiware.github.io/data-models/context.jsonld",
                    "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
                )
            )
        )
    }

    @Test
    fun `it should throw an exception if a JSON-LD input has no @context`() {
        val httpHeaders = HttpHeaders().apply {
            set("Content-Type", "application/ld+json")
        }
        val jsonLdInput = mapOf(
            "id" to "urn:ngsi-ld:Building:farm001",
            "type" to "Building"
        )

        val exception = assertThrows<BadRequestDataException> {
            checkAndGetContext(httpHeaders, jsonLdInput)
        }
        assertEquals(
            "Request payload must contain @context term for a request having an application/ld+json content type",
            exception.message
        )
    }
}
