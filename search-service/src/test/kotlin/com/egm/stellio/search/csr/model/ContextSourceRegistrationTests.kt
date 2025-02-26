package com.egm.stellio.search.csr.model

import com.egm.stellio.search.csr.CsrUtils.gimmeRawCSR
import com.egm.stellio.search.csr.model.ContextSourceRegistration.RegistrationInfo
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.BEEHIVE_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CSR_TERM
import com.egm.stellio.shared.util.MANAGED_BY_COMPACT_RELATIONSHIP
import com.egm.stellio.shared.util.MANAGED_BY_RELATIONSHIP
import com.egm.stellio.shared.util.NGSILD_NAME_PROPERTY
import com.egm.stellio.shared.util.NGSILD_NAME_TERM
import com.egm.stellio.shared.util.expandJsonLdEntity
import com.egm.stellio.shared.util.shouldFailWith
import com.egm.stellio.shared.util.shouldSucceed
import com.egm.stellio.shared.util.shouldSucceedAndResult
import com.egm.stellio.shared.util.toUri
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
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
    fun `getAttributesMatchingCSFAndEntity should get the matching attributes`() = runTest {
        val entity = expandJsonLdEntity(entityPayload)
        val registrationInfoFilter = CSRFilters(
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

        val attrs = csr.getAttributesMatchingCSFAndEntity(registrationInfoFilter, entity)
        assertThat(attrs).contains(NGSILD_NAME_PROPERTY, MANAGED_BY_RELATIONSHIP)
    }

    @Test
    fun `getAttributesMatchingCSFAndEntity should not get Attributes for non matching registrationInfo`() = runTest {
        val entity = expandJsonLdEntity(entityPayload)

        val registrationInfoFilter = CSRFilters(
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

        val attrs = csr.getAttributesMatchingCSFAndEntity(registrationInfoFilter, entity)
        assertThat(attrs).doesNotContain(NGSILD_NAME_PROPERTY, MANAGED_BY_RELATIONSHIP)
    }

    @Test
    fun `getAttributesName should merge propertyNames and relationshipNames`() = runTest {
        val information = RegistrationInfo(
            propertyNames = listOf(NGSILD_NAME_PROPERTY, MANAGED_BY_RELATIONSHIP),
            relationshipNames = listOf(MANAGED_BY_RELATIONSHIP)
        )

        val attrs = information.getAttributeNames()
        assertThat(attrs).hasSize(2)
        assertThat(attrs).contains(NGSILD_NAME_PROPERTY, MANAGED_BY_RELATIONSHIP)
    }

    @Test
    fun `getAttributesName should keep propertyNames if relationShipNames is null`() = runTest {
        val information = RegistrationInfo(
            propertyNames = null,
            relationshipNames = listOf(MANAGED_BY_RELATIONSHIP)
        )

        val attrs = information.getAttributeNames()
        assertEquals(attrs, setOf(MANAGED_BY_RELATIONSHIP))
    }

    @Test
    fun `getAttributesName should keep relationShipNames if propertyNames is null`() = runTest {
        val information = RegistrationInfo(
            propertyNames = listOf(MANAGED_BY_RELATIONSHIP),
            relationshipNames = null
        )

        val attrs = information.getAttributeNames()
        assertEquals(attrs, setOf(MANAGED_BY_RELATIONSHIP))
    }

    @Test
    fun `getAttributesName should return null if propertyNames and relationshipNames are null`() = runTest {
        val information = RegistrationInfo(
            propertyNames = null,
            relationshipNames = null
        )

        val attrs = information.getAttributeNames()
        assertEquals(attrs, null)
    }

    @Test
    fun `computeAttrsQueryParam should intersect the csf and the registration attributes`() = runTest {
        val registrationInfo = RegistrationInfo(
            propertyNames = listOf(MANAGED_BY_RELATIONSHIP, NGSILD_NAME_PROPERTY)
        )
        val csrFilters = CSRFilters(attrs = setOf(NGSILD_NAME_PROPERTY))

        val attrs = registrationInfo.computeAttrsQueryParam(csrFilters, APIC_COMPOUND_CONTEXTS)
        assertEquals(NGSILD_NAME_TERM, attrs)
    }

    @Test
    fun `computeAttrsQueryParam should return the registration attributes if the csf is empty`() = runTest {
        val registrationInfo = RegistrationInfo(
            propertyNames = listOf(MANAGED_BY_RELATIONSHIP, NGSILD_NAME_PROPERTY)
        )
        val csrFilters = CSRFilters()

        val attrs = registrationInfo.computeAttrsQueryParam(csrFilters, APIC_COMPOUND_CONTEXTS)
        assertEquals("$MANAGED_BY_COMPACT_RELATIONSHIP,$NGSILD_NAME_TERM", attrs)
    }

    @Test
    fun `computeAttrsQueryParam should return the csf attributes if the registration have no attributes`() = runTest {
        val registrationInfo = RegistrationInfo()
        val csrFilters = CSRFilters(attrs = setOf(NGSILD_NAME_PROPERTY))

        val attrs = registrationInfo.computeAttrsQueryParam(csrFilters, APIC_COMPOUND_CONTEXTS)
        assertEquals(NGSILD_NAME_TERM, attrs)
    }

    @Test
    fun `toSingleEntityInfoCSRList should return a list of csr with one entityInfo each`() = runTest {
        val registrationInformations = listOf(
            RegistrationInfo(
                entities = listOf(
                    ContextSourceRegistration.EntityInfo(id = "urn:1".toUri(), types = listOf(BEEHIVE_TYPE)),
                    ContextSourceRegistration.EntityInfo(id = "urn:2".toUri(), types = listOf(BEEHIVE_TYPE))
                )
            ),
            RegistrationInfo(
                entities = listOf(
                    ContextSourceRegistration.EntityInfo(id = "urn:3".toUri(), types = listOf(BEEHIVE_TYPE)),
                    ContextSourceRegistration.EntityInfo(id = "urn:4".toUri(), types = listOf(BEEHIVE_TYPE))
                )
            )
        )
        val csr = gimmeRawCSR(information = registrationInformations)

        val csrs = csr.toSingleEntityInfoCSRList(CSRFilters())
        assertThat(csrs).hasSize(4)
        assertThat(csrs).anyMatch { it.information[0].entities?.get(0)?.id == "urn:1".toUri() }
        assertThat(csrs).anyMatch { it.information[0].entities?.get(0)?.id == "urn:2".toUri() }
        assertThat(csrs).anyMatch { it.information[0].entities?.get(0)?.id == "urn:3".toUri() }
        assertThat(csrs).anyMatch { it.information[0].entities?.get(0)?.id == "urn:4".toUri() }
    }

    @Test
    fun `toSingleEntityInfoCSRList should filter the entityInfo`() = runTest {
        val registrationInformations = listOf(
            RegistrationInfo(
                entities = listOf(
                    ContextSourceRegistration.EntityInfo(id = "urn:1".toUri(), types = listOf(BEEHIVE_TYPE)),
                    ContextSourceRegistration.EntityInfo(id = "urn:2".toUri(), types = listOf(BEEHIVE_TYPE))
                )
            ),
            RegistrationInfo(
                entities = listOf(
                    ContextSourceRegistration.EntityInfo(id = "urn:3".toUri(), types = listOf(BEEHIVE_TYPE)),
                    ContextSourceRegistration.EntityInfo(id = "urn:4".toUri(), types = listOf(BEEHIVE_TYPE))
                )
            )
        )
        val csr = gimmeRawCSR(information = registrationInformations)

        val csrs = csr.toSingleEntityInfoCSRList(CSRFilters(ids = setOf("urn:3".toUri())))
        assertThat(csrs).hasSize(1)
        assertThat(csrs).anyMatch { it.information[0].entities?.get(0)?.id == "urn:3".toUri() }
    }

    @Test
    fun `toSingleEntityInfoCSRList should keep registrationInfo without entityInfo`() = runTest {
        val registrationInformations = listOf(
            RegistrationInfo(
                propertyNames = listOf(MANAGED_BY_RELATIONSHIP, NGSILD_NAME_PROPERTY)
            ),
            RegistrationInfo(
                relationshipNames = listOf(MANAGED_BY_RELATIONSHIP)
            ),
            RegistrationInfo(
                propertyNames = listOf(NGSILD_NAME_PROPERTY)
            ),
            RegistrationInfo(
                relationshipNames = listOf(NGSILD_NAME_PROPERTY)
            )
        )
        val csr = gimmeRawCSR(information = registrationInformations)

        val csrs = csr.toSingleEntityInfoCSRList(CSRFilters(attrs = setOf(MANAGED_BY_RELATIONSHIP)))
        assertThat(csrs).hasSize(2)
        assertThat(
            csrs
        ).anyMatch { it.information[0].propertyNames == listOf(MANAGED_BY_RELATIONSHIP, NGSILD_NAME_PROPERTY) }
        assertThat(csrs).anyMatch { it.information[0].relationshipNames == listOf(MANAGED_BY_RELATIONSHIP) }
    }
}
