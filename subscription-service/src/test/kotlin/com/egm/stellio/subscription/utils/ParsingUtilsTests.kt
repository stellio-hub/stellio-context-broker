package com.egm.stellio.subscription.utils

import com.egm.stellio.shared.model.LdContextNotAvailableException
import com.egm.stellio.shared.util.BEEHIVE_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SUBSCRIPTION_TERM
import com.egm.stellio.shared.util.shouldSucceedWith
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.subscription.model.EndpointInfo
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [ParsingUtils::class])
@ActiveProfiles("test")
class ParsingUtilsTests {

    private val beehiveId = "urn:ngsi-ld:BeeHive:TESTC".toUri()

    @Test
    fun `it should correctly parse an endpoint info`() {
        val input = """[{"key": "Authorization-token", "value": "Authorization-token-value"}]"""
        val info = ParsingUtils.parseEndpointInfo(input)

        assertEquals(info, listOf(EndpointInfo(key = "Authorization-token", value = "Authorization-token-value")))
    }

    @Test
    fun `it should correctly parse a null endpoint info`() {
        val input = null
        val info = ParsingUtils.parseEndpointInfo(input)

        assertEquals(info, null)
    }

    @Test
    fun `it should correctly parse a subscription`() = runTest {
        val subscription = mapOf(
            "id" to beehiveId,
            "type" to "Subscription",
            "entities" to listOf(mapOf("type" to BEEHIVE_TYPE)),
            "notification" to mapOf("endpoint" to mapOf("uri" to "http://my.endpoint/notifiy"))
        )

        ParsingUtils.parseSubscription(subscription, emptyList()).shouldSucceedWith {
            assertNotNull(it)
        }
    }

    @Test
    fun `it should not allow a subscription if remote JSON-LD @context cannot be retrieved`() = runTest {
        val contextNonExisting = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-non-existing.jsonld"
        val subscription = mapOf(
            "id" to "urn:ngsi-ld:BeeHive:01",
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "entities" to listOf(mapOf("type" to BEEHIVE_TYPE)),
            "notification" to mapOf("endpoint" to mapOf("uri" to "http://my.endpoint/notifiy"))
        )

        val result = ParsingUtils.parseSubscription(subscription, listOf(contextNonExisting))
        result.fold({
            assertTrue(it is LdContextNotAvailableException)
            assertEquals(
                "Unable to load remote context (cause was: JsonLdError[code=There was a problem encountered " +
                    "loading a remote context [code=LOADING_REMOTE_CONTEXT_FAILED]., message=There was a problem " +
                    "encountered loading a remote context " +
                    "[https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-non-existing.jsonld]])",
                it.message
            )
        }, {
            fail("it should not have allowed a subscription if remote JSON-LD @context cannot be retrieved")
        })
    }
}
