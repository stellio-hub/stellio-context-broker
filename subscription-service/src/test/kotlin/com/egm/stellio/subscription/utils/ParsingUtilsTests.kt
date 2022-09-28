package com.egm.stellio.subscription.utils

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.LdContextNotAvailableException
import com.egm.stellio.shared.util.BEEHIVE_TYPE
import com.egm.stellio.subscription.model.EndpointInfo
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [ParsingUtils::class])
@ActiveProfiles("test")
class ParsingUtilsTests {

    @Test
    fun `it should correctly parse an endpoint info`() {
        val input = "[{\"key\": \"Authorization-token\", \"value\": \"Authorization-token-value\"}]"
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
    fun `it should not allow a subscription with an empty id`() {
        val subscription = mapOf(
            "id" to "",
            "type" to "Subscription",
            "entities" to listOf(mapOf("type" to BEEHIVE_TYPE)),
            "notification" to mapOf("endpoint" to mapOf("uri" to "http://my.endpoint/notifiy"))
        )

        runBlocking {
            val result = ParsingUtils.parseSubscription(subscription, emptyList())
            result.fold({
                assertTrue(it is BadRequestDataException)
                assertEquals("The supplied identifier was expected to be an URI but it is not: ", it.message)
            }, {
                fail("it should not have allowed a subscription with an empty id")
            })
        }
    }

    @Test
    fun `it should not allow a subscription with an invalid id`() {
        val subscription = mapOf(
            "id" to "invalidId",
            "type" to "Subscription",
            "entities" to listOf(mapOf("type" to BEEHIVE_TYPE)),
            "notification" to mapOf("endpoint" to mapOf("uri" to "http://my.endpoint/notifiy"))
        )

        runBlocking {
            val result = ParsingUtils.parseSubscription(subscription, emptyList())
            result.fold({
                assertTrue(it is BadRequestDataException)
                assertEquals("The supplied identifier was expected to be an URI but it is not: invalidId", it.message)
            }, {
                fail("it should not have allowed a subscription with an invalid id")
            })
        }
    }

    @Test
    fun `it should not allow a subscription if remote JSON-LD @context cannot be retrieved`() = runTest {
        val contextNonExisting = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-non-existing.jsonld"
        val subscription = mapOf(
            "id" to "urn:ngsi-ld:BeeHive:01",
            "type" to "Subscription",
            "entities" to listOf(mapOf("type" to BEEHIVE_TYPE)),
            "notification" to mapOf("endpoint" to mapOf("uri" to "http://my.endpoint/notifiy"))
        )

        val result = ParsingUtils.parseSubscription(subscription, listOf(contextNonExisting))
        result.fold({
            assertTrue(it is LdContextNotAvailableException)
            assertEquals(
                "Unable to load remote context (cause was: com.github.jsonldjava.core.JsonLdError: " +
                    "loading remote context failed: $contextNonExisting)",
                it.message
            )
        }, {
            fail("it should not have allowed a subscription if remote JSON-LD @context cannot be retrieved")
        })
    }
}
