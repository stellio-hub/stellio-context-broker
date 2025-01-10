package com.egm.stellio.shared.util

import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.InvalidRequestException
import com.egm.stellio.shared.queryparameter.OptionsValue.TEMPORAL_VALUES
import com.egm.stellio.shared.web.CustomWebFilter
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
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

    private val applicationProperties = mockk<ApplicationProperties> {
        every { contexts.core } returns "http://localhost:8093/jsonld-contexts/ngsi-ld-core-context-v1.8.jsonld"
    }

    @Test
    fun `it should not find a value if there is no options query param`() {
        val result = hasValueInOptionsParam(Optional.empty(), TEMPORAL_VALUES).shouldSucceedAndResult()
        assertFalse(result)
    }

    @Test
    fun `it should return an exception if it is given an invalid options query param`() {
        hasValueInOptionsParam(Optional.of("one"), TEMPORAL_VALUES).shouldFail {
            assertInstanceOf(InvalidRequestException::class.java, it)
            assertEquals("'one' is not a valid value for the options query parameter", it.message)
        }
    }

    @Test
    fun `it should return an exception if it is given an invalid multi value options query param`() {
        hasValueInOptionsParam(Optional.of("one,two"), TEMPORAL_VALUES).shouldFail {
            assertInstanceOf(InvalidRequestException::class.java, it)
            assertEquals("'one' is not a valid value for the options query parameter", it.message)
        }
    }

    @Test
    fun `it should find a value if it is in a single value options query param`() {
        val result = hasValueInOptionsParam(Optional.of("temporalValues"), TEMPORAL_VALUES).shouldSucceedAndResult()
        assertTrue(result)
    }

    @Test
    fun `it should return an exception if it is given at least one invalid value in options query param`() {
        hasValueInOptionsParam(Optional.of("one,temporalValues"), TEMPORAL_VALUES).shouldFail {
            assertInstanceOf(InvalidRequestException::class.java, it)
            assertEquals("'one' is not a valid value for the options query parameter", it.message)
        }
    }

    @Test
    fun `it should return an empty list if no attrs param is provided`() {
        assertTrue(parseAndExpandQueryParameter(null, emptyList()).isEmpty())
    }

    @Test
    fun `it should return an singleton list if there is one provided attrs param`() {
        assertEquals(1, parseAndExpandQueryParameter("attr1", NGSILD_TEST_CORE_CONTEXTS).size)
    }

    @Test
    fun `it should return a list with two elements if there are two provided attrs param`() {
        assertEquals(2, parseAndExpandQueryParameter("attr1, attr2", NGSILD_TEST_CORE_CONTEXTS).size)
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
        getContextFromLinkHeader(listOf(APIC_HEADER_LINK)).shouldSucceedWith {
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
        getContextFromLinkHeader(APIC_COMPOUND_CONTEXTS).shouldFail {
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
        getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).shouldSucceedWith {
            assertEquals(listOf(applicationProperties.contexts.core), it)
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
            "@context" to NGSILD_TEST_CORE_CONTEXT
        )

        checkAndGetContext(httpHeaders, jsonLdInput, applicationProperties.contexts.core).shouldSucceedWith {
            assertEquals(1, it.size)
            assertEquals(NGSILD_TEST_CORE_CONTEXT, it[0])
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

        checkAndGetContext(httpHeaders, jsonLdInput, applicationProperties.contexts.core).shouldSucceedWith {
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

        checkAndGetContext(httpHeaders, jsonLdInput, applicationProperties.contexts.core).shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals(
                "Request payload must contain @context term for a request having an application/ld+json content type",
                it.message
            )
        }
    }

    @Test
    fun `it should find a JSON-LD @context in an input map`() {
        val input = mapOf(
            "id" to "urn:ngsi-ld:Entity:1",
            "@context" to "https://some.context"
        )

        assertEquals(listOf("https://some.context"), input.extractContexts())
    }

    @Test
    fun `it should find a list of JSON-LD @contexts in an input map`() {
        val input = mapOf(
            "id" to "urn:ngsi-ld:Entity:1",
            "@context" to listOf("https://some.context", "https://some.context.2")
        )

        assertEquals(listOf("https://some.context", "https://some.context.2"), input.extractContexts())
    }

    @Test
    fun `it should return an empty list if no JSON-LD @context was found an input map`() {
        val input = mapOf(
            "id" to "urn:ngsi-ld:Entity:1"
        )

        assertEquals(emptyList<String>(), input.extractContexts())
    }

    @Test
    fun `it should return the core context if the list of contexts is empty`() {
        val contexts = addCoreContextIfMissing(emptyList(), applicationProperties.contexts.core)
        assertEquals(1, contexts.size)
        assertEquals(listOf(applicationProperties.contexts.core), contexts)
    }

    @Test
    fun `it should move the core context at last position if it is not last in the list of contexts`() {
        val contexts = addCoreContextIfMissing(
            listOf(applicationProperties.contexts.core).plus(APIC_COMPOUND_CONTEXTS),
            applicationProperties.contexts.core
        )
        assertEquals(2, contexts.size)
        assertEquals(APIC_COMPOUND_CONTEXTS.plus(applicationProperties.contexts.core), contexts)
    }

    @Test
    fun `it should add the core context at last position if it is not in the list of contexts`() {
        val contexts = addCoreContextIfMissing(listOf(APIC_CONTEXT), applicationProperties.contexts.core)
        assertEquals(2, contexts.size)
        assertEquals(listOf(APIC_CONTEXT, applicationProperties.contexts.core), contexts)
    }

    @Test
    fun `it should not add the core context if it resolvable from the provided contexts`() {
        val contexts = addCoreContextIfMissing(
            listOf("https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.4.jsonld"),
            applicationProperties.contexts.core
        )
        assertEquals(1, contexts.size)
        assertEquals(listOf("https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.4.jsonld"), contexts)
    }

    @Test
    fun `it should parse and expand entity type selection query`() {
        val query = "(TypeA|TypeB);(TypeC,TypeD)"
        val defaultExpand = "https://uri.etsi.org/ngsi-ld/default-context/"
        val expandedQuery = expandTypeSelection(query, NGSILD_TEST_CORE_CONTEXTS)
        val expectedExpandTypeSelection =
            "(${defaultExpand}TypeA|${defaultExpand}TypeB);(${defaultExpand}TypeC,${defaultExpand}TypeD)"
        assertEquals(expectedExpandTypeSelection, expandedQuery)
    }

    @Test
    fun `it should parse a conform datetime parameter`() {
        val parseResult = "2023-10-16T16:18:00Z".parseTimeParameter("Invalid date time")
        parseResult.onLeft {
            fail("it should have parsed the date time")
        }
    }

    @Test
    fun `it should return an error if datetime parameter is not conform`() {
        val parseResult = "16/10/2023".parseTimeParameter("Invalid date time")
        parseResult.fold(
            { assertEquals("Invalid date time", it) }
        ) {
            fail("it should not have parsed the date time")
        }
    }

    @Test
    fun `it should validate a correct idPattern`() {
        val validationResult = validateIdPattern("urn:ngsi-ld:Entity:*")
        validationResult.onLeft {
            fail("it should have parsed the date time")
        }
    }

    @Test
    fun `it should not validate an incorrect idPattern`() {
        val validationResult = validateIdPattern("(?x)urn:ngsi-ld:Entity:{*}2")
        validationResult.fold(
            { assertTrue(it.message.startsWith("Invalid value for idPattern: (?x)urn:ngsi-ld:Entity:{*}2")) },
            { fail("it should not have validated the idPattern") }
        )
    }
}
