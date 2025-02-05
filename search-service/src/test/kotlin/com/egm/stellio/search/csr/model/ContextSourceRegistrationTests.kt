package com.egm.stellio.search.csr.model

import com.egm.stellio.search.csr.model.ContextSourceRegistration.RegistrationInfo
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.BEEHIVE_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CSR_TERM
import com.egm.stellio.shared.util.MANAGED_BY_RELATIONSHIP
import com.egm.stellio.shared.util.NGSILD_NAME_PROPERTY
import com.egm.stellio.shared.util.expandJsonLdEntity
import com.egm.stellio.shared.util.shouldFailWith
import com.egm.stellio.shared.util.shouldSucceed
import com.egm.stellio.shared.util.shouldSucceedAndResult
import com.egm.stellio.shared.util.toUri
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class ContextSourceRegistrationTests {

    private val endpoint = "http://my.csr.endpoint/"

    private val entityPayload = """
        {
            "id": "urn:ngsi-ld:Entity:01",
            "type": "Entity",
            "name": {
                "type": "Property",
                "value": "An entity"
            },
            "managedBy": {
                 "type": "Relationship",
                 "datasetId": "urn:ngsi-ld:Dataset:french-name",
                 "object": "urn:ngsi-ld:Apiculteur:1230"
            },
            "@context": [ "$APIC_COMPOUND_CONTEXT" ]
        }
    """.trimIndent()

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

    @Test
    fun `getAssociatedAttributes should check properties and relationship separately`() = runTest {
        val entity = expandJsonLdEntity(entityPayload)
        val registrationInfoFilter = RegistrationInfoFilter(
            ids = setOf(entity.id),
            types = entity.types.toSet()
        )
        val entityInfo = ContextSourceRegistration.EntityInfo(entity.id, types = entity.types)
        val information = RegistrationInfo(
            entities = listOf(entityInfo),
            propertyNames = listOf(NGSILD_NAME_PROPERTY),
            relationshipNames = listOf(MANAGED_BY_RELATIONSHIP)
        )

        val csr = ContextSourceRegistration(
            endpoint = "http://my:csr".toUri(),
            information = listOf(information)
        )

        val attrs = csr.getAssociatedAttributes(registrationInfoFilter, entity)
        assertThat(attrs).contains(NGSILD_NAME_PROPERTY, MANAGED_BY_RELATIONSHIP)

        val invertedCsr = ContextSourceRegistration(
            endpoint = "http://my:csr".toUri(),
            information = listOf(
                RegistrationInfo(
                    entities = listOf(entityInfo),
                    propertyNames = listOf(MANAGED_BY_RELATIONSHIP),
                    relationshipNames = listOf(NGSILD_NAME_PROPERTY)
                )
            )
        )
        val inversedAttrs = invertedCsr.getAssociatedAttributes(registrationInfoFilter, entity)

        assertThat(inversedAttrs).doesNotContain(NGSILD_NAME_PROPERTY, MANAGED_BY_RELATIONSHIP)
    }

    @Test
    fun `getAssociatedAttributes should not get Attributes for non matching registrationInfo`() = runTest {
        val entity = expandJsonLdEntity(entityPayload)

        val registrationInfoFilter = RegistrationInfoFilter(
            ids = setOf(entity.id),
            types = entity.types.toSet()
        )
        val nonMatchingEntityInfo = ContextSourceRegistration.EntityInfo(types = listOf(BEEHIVE_TYPE))
        val nonMatchingInformation = RegistrationInfo(
            entities = listOf(nonMatchingEntityInfo),
            propertyNames = listOf(NGSILD_NAME_PROPERTY),
            relationshipNames = listOf(MANAGED_BY_RELATIONSHIP)
        )

        val csr = ContextSourceRegistration(
            endpoint = "http://my:csr".toUri(),
            information = listOf(nonMatchingInformation)
        )

        val attrs = csr.getAssociatedAttributes(registrationInfoFilter, entity)
        assertThat(attrs).doesNotContain(NGSILD_NAME_PROPERTY, MANAGED_BY_RELATIONSHIP)
    }
}
