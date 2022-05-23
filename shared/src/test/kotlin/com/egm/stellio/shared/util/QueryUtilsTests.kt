package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles
import org.springframework.util.LinkedMultiValueMap

@ActiveProfiles("test")
class QueryUtilsTests {

    @Test
    fun `it should parse query parameters`() {
        val requestParams = gimmeFullParamsMap()
        val queryParams = parseAndCheckParams(Pair(1, 20), requestParams, APIC_COMPOUND_CONTEXT)

        Assertions.assertEquals(setOf(BEEHIVE_TYPE, APIARY_TYPE), queryParams.types)
        Assertions.assertEquals(setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY), queryParams.attrs)
        Assertions.assertEquals(
            setOf("urn:ngsi-ld:BeeHive:TESTC".toUri(), "urn:ngsi-ld:BeeHive:TESTB".toUri()),
            queryParams.ids
        )
        Assertions.assertEquals(".*BeeHive.*", queryParams.idPattern)
        Assertions.assertEquals("brandName!=Mercedes", queryParams.q)
        Assertions.assertEquals(true, queryParams.count)
        Assertions.assertEquals(1, queryParams.offset)
        Assertions.assertEquals(10, queryParams.limit)
        Assertions.assertEquals(true, queryParams.useSimplifiedRepresentation)
        Assertions.assertEquals(false, queryParams.includeSysAttrs)
    }

    @Test
    fun `it should set includeSysAttrs at true if options contains includeSysAttrs query parameters`() {
        val requestParams = LinkedMultiValueMap<String, String>()
        requestParams.add("options", "sysAttrs")
        val queryParams = parseAndCheckParams(Pair(30, 100), requestParams, NGSILD_CORE_CONTEXT)

        Assertions.assertEquals(true, queryParams.includeSysAttrs)
    }

    @Test
    fun `it should decode q in query parameters`() {
        val requestParams = LinkedMultiValueMap<String, String>()
        requestParams.add("q", "speed%3E50%3BfoodName%3D%3Ddietary+fibres")
        val queryParams = parseAndCheckParams(Pair(30, 100), requestParams, NGSILD_CORE_CONTEXT)

        Assertions.assertEquals("speed>50;foodName==dietary fibres", queryParams.q)
    }

    @Test
    fun `it should set default values in query parameters`() {
        val requestParams = LinkedMultiValueMap<String, String>()
        val queryParams = parseAndCheckParams(Pair(30, 100), requestParams, NGSILD_CORE_CONTEXT)

        Assertions.assertEquals(emptySet<String>(), queryParams.types)
        Assertions.assertEquals(emptySet<String>(), queryParams.attrs)
        Assertions.assertEquals(emptySet<String>(), queryParams.ids)
        Assertions.assertEquals(null, queryParams.idPattern)
        Assertions.assertEquals(null, queryParams.q)
        Assertions.assertEquals(false, queryParams.count)
        Assertions.assertEquals(0, queryParams.offset)
        Assertions.assertEquals(30, queryParams.limit)
        Assertions.assertEquals(false, queryParams.useSimplifiedRepresentation)
        Assertions.assertEquals(false, queryParams.includeSysAttrs)
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
