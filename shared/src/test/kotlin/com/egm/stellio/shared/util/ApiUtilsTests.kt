package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.OptionsParamValue.TEMPORAL_VALUES
import com.egm.stellio.shared.web.CustomWebFilter
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import java.util.Optional

@OptIn(ExperimentalCoroutinesApi::class)
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
    fun `it should extract a @context from a valid Link header`() = runTest {
        getContextFromLinkHeader(listOf(buildContextLinkHeader(APIC_COMPOUND_CONTEXT))).shouldSucceedWith {
            assertNotNull(it)
            assertEquals(APIC_COMPOUND_CONTEXT, it)
        }
    }

    @Test
    fun `it should return a null @context if no Link header was provided`() = runTest {
        getContextFromLinkHeader(emptyList()).shouldSucceedWith {
            assertNull(it)
        }
    }

    @Test
    fun `it should return a BadRequestData error if @context provided in Link header is invalid`() = runTest {
        getContextFromLinkHeader(listOf(APIC_COMPOUND_CONTEXT)).shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals(
                "Badly formed Link header: $APIC_COMPOUND_CONTEXT",
                it.message
            )
        }
    }

    @Test
    fun `it should return the default @context if no Link header was provided and default is asked`() = runTest {
        val httpHeaders = HttpHeaders()
        getContextFromLinkHeaderOrDefault(httpHeaders).shouldSucceedWith {
            assertEquals(NGSILD_CORE_CONTEXT, it)
        }
    }

    @Test
    fun `it should get a single @context from a JSON-LD input`() = runTest {
        val httpHeaders = HttpHeaders().apply {
            set("Content-Type", "application/ld+json")
        }
        val jsonLdInput = mapOf(
            "id" to "urn:ngsi-ld:Building:farm001",
            "type" to "Building",
            "@context" to "https://fiware.github.io/data-models/context.jsonld"
        )

        checkAndGetContext(httpHeaders, jsonLdInput).shouldSucceedWith {
            assertEquals(1, it.size)
            assertEquals("https://fiware.github.io/data-models/context.jsonld", it[0])
        }
    }

    @Test
    fun `it should get a list of @context from a JSON-LD input`() = runTest {
        val httpHeaders = HttpHeaders().apply {
            set("Content-Type", "application/ld+json")
        }
        val jsonLdInput = mapOf(
            "id" to "urn:ngsi-ld:Building:farm001",
            "type" to "Building",
            "@context" to listOf(
                "https://fiware.github.io/data-models/context.jsonld",
                "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
            )
        )

        checkAndGetContext(httpHeaders, jsonLdInput).shouldSucceedWith {
            assertEquals(2, it.size)
            assertTrue(
                it.containsAll(
                    listOf(
                        "https://fiware.github.io/data-models/context.jsonld",
                        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
                    )
                )
            )
        }
    }

    @Test
    fun `it should throw an exception if a JSON-LD input has no @context`() = runTest {
        val httpHeaders = HttpHeaders().apply {
            set("Content-Type", "application/ld+json")
        }
        val jsonLdInput = mapOf(
            "id" to "urn:ngsi-ld:Building:farm001",
            "type" to "Building"
        )

        checkAndGetContext(httpHeaders, jsonLdInput).shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals(
                "Request payload must contain @context term for a request having an application/ld+json content type",
                it.message
            )
        }
    }

    @Test
    fun `it should parse and expand entity type selection query`() {
        val query = "(TypeA|TypeB);(TypeC,TypeD)"
        val defaultExpand = "https://uri.etsi.org/ngsi-ld/default-context/"
        val expandedQuery = parseAndExpandTypeSelection(query, NGSILD_CORE_CONTEXT)
        val expectedExpandTypeSelection =
            "(${defaultExpand}TypeA|${defaultExpand}TypeB);(${defaultExpand}TypeC,${defaultExpand}TypeD)"
        assertEquals(expectedExpandTypeSelection, expandedQuery)
    }
}
