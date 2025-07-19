package com.egm.stellio.search.csr.model

import com.egm.stellio.search.csr.CsrUtils.gimmeRawCSR
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.NGSILD_CSR_TERM
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.MANAGED_BY_IRI
import com.egm.stellio.shared.util.NAME_IRI
import com.egm.stellio.shared.util.expandJsonLdEntity
import com.egm.stellio.shared.util.shouldFailWith
import com.egm.stellio.shared.util.shouldSucceed
import com.egm.stellio.shared.util.shouldSucceedAndResult
import com.egm.stellio.shared.util.toUri
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.net.URI

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
            "information" to listOf(mapOf("entities" to listOf(mapOf("idPattern" to "[", "type" to BEEHIVE_IRI)))),
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
            "information" to listOf(mapOf("entities" to listOf(mapOf("type" to BEEHIVE_IRI)))),
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
    fun `getAttributesMatchingCSFAndEntity should get the matching attributes`() = runTest {
        val entity = expandJsonLdEntity(entityPayload)
        val registrationInfoFilter = CSRFilters(
            ids = setOf(entity.id),
            types = entity.types.toSet()
        )
        val entityInfo = EntityInfo(entity.id, types = entity.types)
        val information = RegistrationInfo(
            entities = listOf(entityInfo),
            propertyNames = listOf(NAME_IRI),
            relationshipNames = listOf(MANAGED_BY_IRI)
        )

        val csr = ContextSourceRegistration(
            endpoint = "http://my:csr".toUri(),
            information = listOf(information)
        )

        val attrs = csr.getAttributesMatchingCSFAndEntity(registrationInfoFilter, entity)
        assertThat(attrs).contains(NAME_IRI, MANAGED_BY_IRI)
    }

    @Test
    fun `getAttributesMatchingCSFAndEntity should not get attributes for non matching registrationInfo`() = runTest {
        val entity = expandJsonLdEntity(entityPayload)

        val registrationInfoFilter = CSRFilters(
            ids = setOf(entity.id),
            types = entity.types.toSet()
        )
        val nonMatchingEntityInfo = EntityInfo(types = listOf(BEEHIVE_IRI))
        val nonMatchingInformation = RegistrationInfo(
            entities = listOf(nonMatchingEntityInfo),
            propertyNames = listOf(NAME_IRI),
            relationshipNames = listOf(MANAGED_BY_IRI)
        )

        val csr = ContextSourceRegistration(
            endpoint = "http://my:csr".toUri(),
            information = listOf(nonMatchingInformation)
        )

        val attrs = csr.getAttributesMatchingCSFAndEntity(registrationInfoFilter, entity)
        assertThat(attrs).doesNotContain(NAME_IRI, MANAGED_BY_IRI)
    }

    @Test
    fun `toSingleEntityInfoCSRList should return a list of csr with one entityInfo each`() = runTest {
        val registrationInformations = listOf(
            RegistrationInfo(
                entities = listOf(
                    EntityInfo(id = "urn:1".toUri(), types = listOf(BEEHIVE_IRI)),
                    EntityInfo(id = "urn:2".toUri(), types = listOf(BEEHIVE_IRI))
                )
            ),
            RegistrationInfo(
                entities = listOf(
                    EntityInfo(id = "urn:3".toUri(), types = listOf(BEEHIVE_IRI)),
                    EntityInfo(id = "urn:4".toUri(), types = listOf(BEEHIVE_IRI))
                )
            )
        )
        val csr = gimmeRawCSR(information = registrationInformations)

        val csrs = csr.toSingleEntityInfoCSRList(CSRFilters())
        assertThat(csrs).hasSize(4)
            .extracting<URI> { it.information[0].entities?.get(0)?.id }
            .contains("urn:1".toUri(), "urn:2".toUri(), "urn:3".toUri(), "urn:4".toUri())
    }

    @Test
    fun `toSingleEntityInfoCSRList should filter the entityInfo`() = runTest {
        val registrationInformations = listOf(
            RegistrationInfo(
                entities = listOf(
                    EntityInfo(id = "urn:1".toUri(), types = listOf(BEEHIVE_IRI)),
                    EntityInfo(id = "urn:2".toUri(), types = listOf(BEEHIVE_IRI))
                )
            ),
            RegistrationInfo(
                entities = listOf(
                    EntityInfo(id = "urn:3".toUri(), types = listOf(BEEHIVE_IRI)),
                    EntityInfo(id = "urn:4".toUri(), types = listOf(BEEHIVE_IRI))
                )
            )
        )
        val csr = gimmeRawCSR(information = registrationInformations)

        val csrs = csr.toSingleEntityInfoCSRList(CSRFilters(ids = setOf("urn:3".toUri())))
        assertThat(csrs).hasSize(1)
            .first()
            .matches { it.information[0].entities?.get(0)?.id == "urn:3".toUri() }
    }

    @Test
    fun `toSingleEntityInfoCSRList should keep registrationInfo without entityInfo`() = runTest {
        val registrationInformations = listOf(
            RegistrationInfo(
                propertyNames = listOf(MANAGED_BY_IRI, NAME_IRI)
            ),
            RegistrationInfo(
                relationshipNames = listOf(MANAGED_BY_IRI)
            ),
            RegistrationInfo(
                propertyNames = listOf(NAME_IRI)
            ),
            RegistrationInfo(
                relationshipNames = listOf(NAME_IRI)
            )
        )
        val csr = gimmeRawCSR(information = registrationInformations)

        val csrs = csr.toSingleEntityInfoCSRList(CSRFilters(attrs = setOf(MANAGED_BY_IRI)))
        assertThat(csrs).hasSize(2)
            .allMatch {
                it.information[0].propertyNames == listOf(MANAGED_BY_IRI, NAME_IRI) ||
                    it.information[0].relationshipNames == listOf(MANAGED_BY_IRI)
            }
    }
}
