package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.OptionsParamValue.TEMPORAL_VALUES
import com.egm.stellio.shared.web.CustomWebFilter
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.util.LinkedMultiValueMap
import java.net.URI
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
                "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld"
            )
        )

        val contexts = checkAndGetContext(httpHeaders, jsonLdInput)
        assertEquals(2, contexts.size)
        assertTrue(
            contexts.containsAll(
                listOf(
                    "https://fiware.github.io/data-models/context.jsonld",
                    "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld"
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

    @Test
    fun `it should parse query parameters`() {
        val requestParams = gimmeFullParamsMap()
        val queryParams = parseAndCheckParams(Pair(1, 20), requestParams, APIC_COMPOUND_CONTEXT)

        assertEquals(setOf(BEEHIVE_TYPE, APIARY_TYPE), queryParams.types)
        assertEquals(setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY), queryParams.attrs)
        assertEquals(
            setOf("urn:ngsi-ld:BeeHive:TESTC".toUri(), "urn:ngsi-ld:BeeHive:TESTB".toUri()),
            queryParams.ids
        )
        assertEquals(".*BeeHive.*", queryParams.idPattern)
        assertEquals("brandName!=Mercedes", queryParams.q)
        assertEquals(true, queryParams.count)
        assertEquals(1, queryParams.offset)
        assertEquals(10, queryParams.limit)
        assertEquals(true, queryParams.useSimplifiedRepresentation)
        assertEquals(false, queryParams.includeSysAttrs)
    }

    @Test
    fun `it should set includeSysAttrs at true if options contains includeSysAttrs query parameters`() {
        val requestParams = LinkedMultiValueMap<String, String>()
        requestParams.add("options", "sysAttrs")
        val queryParams = parseAndCheckParams(Pair(30, 100), requestParams, NGSILD_CORE_CONTEXT)

        assertEquals(true, queryParams.includeSysAttrs)
    }

    @Test
    fun `it should decode q in query parameters`() {
        val requestParams = LinkedMultiValueMap<String, String>()
        requestParams.add("q", "speed%3E50%3BfoodName%3D%3Ddietary+fibres")
        val queryParams = parseAndCheckParams(Pair(30, 100), requestParams, NGSILD_CORE_CONTEXT)

        assertEquals("speed>50;foodName==dietary fibres", queryParams.q)
    }

    @Test
    fun `it should set default values in query parameters`() {
        val requestParams = LinkedMultiValueMap<String, String>()
        val queryParams = parseAndCheckParams(Pair(30, 100), requestParams, NGSILD_CORE_CONTEXT)

        assertEquals(emptySet<String>(), queryParams.types)
        assertEquals(emptySet<String>(), queryParams.attrs)
        assertEquals(emptySet<URI>(), queryParams.ids)
        assertEquals(null, queryParams.idPattern)
        assertEquals(null, queryParams.q)
        assertEquals(false, queryParams.count)
        assertEquals(0, queryParams.offset)
        assertEquals(30, queryParams.limit)
        assertEquals(false, queryParams.useSimplifiedRepresentation)
        assertEquals(false, queryParams.includeSysAttrs)
    }

    private fun gimmeFullParamsMap(): LinkedMultiValueMap<String, String> {
        val requestParams = LinkedMultiValueMap<String, String>()
        requestParams.add("type", "BeeHive,Apiary")
        requestParams.add("attrs", "incoming,outgoing")
        requestParams.add("id", "urn:ngsi-ld:BeeHive:TESTC,urn:ngsi-ld:BeeHive:TESTB")
        requestParams.add("idPattern", ".*BeeHive.*")
        requestParams.add("q", "brandName!=Mercedes")
        requestParams.add("count", "true")
        requestParams.add("offset", "1")
        requestParams.add("limit", "10")
        requestParams.add("options", "keyValues")
        return requestParams
    }
}
