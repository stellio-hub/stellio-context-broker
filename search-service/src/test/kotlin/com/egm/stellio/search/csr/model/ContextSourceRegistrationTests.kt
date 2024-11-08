package com.egm.stellio.search.csr.model

import com.egm.stellio.search.csr.model.ContextSourceRegistration.RegistrationInfo
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.BEEHIVE_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CSR_TERM
import com.egm.stellio.shared.util.shouldFailWith
import com.egm.stellio.shared.util.shouldSucceed
import com.egm.stellio.shared.util.shouldSucceedAndResult
import com.egm.stellio.shared.util.toUri
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

class ContextSourceRegistrationTests {

    private val endpoint = "http://my.csr.endpoint/"

    @Test
    fun `it should not allow a CSR with an empty id`() = runTest {
        val payload = mapOf(
            "id" to "",
            "type" to NGSILD_CSR_TERM,
            "information" to emptyList<RegistrationInfo>(),
            "endpoint" to endpoint
        )

        val contextSourceRegistration = ContextSourceRegistration.deserialize(
            payload,
            emptyList()
        ).shouldSucceedAndResult()
        contextSourceRegistration.validate()
            .shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "The supplied identifier was expected to be an URI but it is not: "
            }
    }

    @Test
    fun `it should not allow a CSR with an invalid id`() = runTest {
        val payload = mapOf(
            "id" to "invalidId",
            "type" to NGSILD_CSR_TERM,
            "information" to emptyList<RegistrationInfo>(),
            "endpoint" to endpoint
        )

        val contextSourceRegistration = ContextSourceRegistration.deserialize(
            payload,
            emptyList()
        ).shouldSucceedAndResult()
        contextSourceRegistration.validate()
            .shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "The supplied identifier was expected to be an URI but it is not: invalidId"
            }
    }

    @Test
    fun `it should not allow a CSR with an invalid idPattern`() = runTest {
        val payload = mapOf(
            "id" to "urn:ngsi-ld:Beehive:1234567890".toUri(),
            "type" to NGSILD_CSR_TERM,
            "information" to listOf(mapOf("entities" to listOf(mapOf("idPattern" to "[", "type" to BEEHIVE_TYPE)))),
            "endpoint" to endpoint
        )

        val contextSourceRegistration = ContextSourceRegistration.deserialize(
            payload,
            emptyList()
        ).shouldSucceedAndResult()
        contextSourceRegistration.validate()
            .shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "Invalid idPattern found in contextSourceRegistration"
            }
    }

    @Test
    fun `it should not allow a CSR with empty RegistrationInfo`() = runTest {
        val payload = mapOf(
            "id" to "urn:ngsi-ld:Beehive:1234567890".toUri(),
            "information" to listOf(mapOf<String, Any>()),
            "type" to NGSILD_CSR_TERM,
            "endpoint" to endpoint
        )

        val contextSourceRegistration = ContextSourceRegistration.deserialize(
            payload,
            emptyList()
        ).shouldSucceedAndResult()
        contextSourceRegistration.validate()
            .shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "RegistrationInfo should have at least one element"
            }
    }

    @Test
    fun `it should allow a valid CSR`() = runTest {
        val payload = mapOf(
            "id" to "urn:ngsi-ld:Beehive:1234567890".toUri(),
            "information" to listOf(mapOf("entities" to listOf(mapOf("type" to BEEHIVE_TYPE)))),
            "type" to NGSILD_CSR_TERM,
            "endpoint" to endpoint
        )

        val contextSourceRegistration = ContextSourceRegistration.deserialize(
            payload,
            emptyList()
        ).shouldSucceedAndResult()
        contextSourceRegistration.validate()
            .shouldSucceed()
    }
}
