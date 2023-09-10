package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

@OptIn(ExperimentalCoroutinesApi::class)
class DataRepresentationUtilsTests {

    @Test
    fun `it should not validate an entity with an invalid type name`() = runTest {
        val rawEntity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Invalid(Type)"
            }
            """.trimIndent()

        rawEntity.deserializeAsMap().checkNamesAreNgsiLdSupported()
            .shouldFail {
                assertInstanceOf(BadRequestDataException::class.java, it)
                assertEquals(
                    "The JSON-LD object contains a member with invalid characters (4.6.2): Invalid(Type)",
                    it.message
                )
            }
    }

    @Test
    fun `it should not validate an entity with one invalid type name in a list of many`() = runTest {
        val rawEntity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": ["Invalid(Type)", "CorrectType"]
            }
            """.trimIndent()

        rawEntity.deserializeAsMap().checkNamesAreNgsiLdSupported()
            .shouldFail {
                assertInstanceOf(BadRequestDataException::class.java, it)
                assertEquals(
                    "The JSON-LD object contains a member with invalid characters (4.6.2): Invalid(Type)",
                    it.message
                )
            }
    }

    @Test
    fun `it should validate an entity with allowed characters for attribute name`() = runTest {
        val rawEntity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Property",
                "prefix:device_state": {
                    "type": "Property",
                    "value": 23
                }
            }
            """.trimIndent()

        rawEntity.deserializeAsMap().checkNamesAreNgsiLdSupported().shouldSucceed()
    }

    @Test
    fun `it should not validate an entity with an invalid attribute name`() = runTest {
        val rawEntity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device",
                "device<State": {
                    "type": "Property",
                    "value": 23
                }
            }
            """.trimIndent()

        rawEntity.deserializeAsMap().checkNamesAreNgsiLdSupported()
            .shouldFail {
                assertInstanceOf(BadRequestDataException::class.java, it)
                assertEquals(
                    "The JSON-LD object contains a member with invalid characters (4.6.2): device<State",
                    it.message
                )
            }
    }

    @Test
    fun `it should not validate an entity with an invalid sub-attribute name`() = runTest {
        val rawEntity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device",
                "device": {
                    "type": "Property",
                    "value": 23,
                    "device<State": {
                        "type": "Property",
                        "value": "open"
                    }
                }
            }
            """.trimIndent()

        rawEntity.deserializeAsMap().checkNamesAreNgsiLdSupported()
            .shouldFail {
                assertInstanceOf(BadRequestDataException::class.java, it)
                assertEquals(
                    "The JSON-LD object contains a member with invalid characters (4.6.2): device<State",
                    it.message
                )
            }
    }

    @Test
    fun `it should not validate an entity with an invalid sub-attribute name in a multi-attribute`() = runTest {
        val rawEntity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device",
                "device": [{
                    "type": "Property",
                    "value": 23,
                    "state": {
                        "type": "Property",
                        "value": "open"
                    }
                },{
                    "type": "Property",
                    "value": 23,
                    "device<State": {
                        "type": "Property",
                        "value": "open"
                    }                
                }]
            }
            """.trimIndent()

        rawEntity.deserializeAsMap().checkNamesAreNgsiLdSupported()
            .shouldFail {
                assertInstanceOf(BadRequestDataException::class.java, it)
                assertEquals(
                    "The JSON-LD object contains a member with invalid characters (4.6.2): device<State",
                    it.message
                )
            }
    }

    @ParameterizedTest
    @CsvSource(
        "#Scope",
        "/3Scope",
        "/Scope#Another"
    )
    fun `it should not validate an invalid scope name`(scope: String) = runTest {
        scope.checkScopesNamesAreNgsiLdSupported().shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals(
                "The JSON-LD object contains a scope with invalid characters (4.18): $scope",
                it.message
            )
        }
    }

    @Test
    fun `it should not validate invalid scopes names`() = runTest {
        listOf("/Scope/#Scope", "/A,/B").checkScopesNamesAreNgsiLdSupported().shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals(
                "The JSON-LD object contains a scope with invalid characters (4.18): /Scope/#Scope",
                it.message
            )
        }
    }

    @ParameterizedTest
    @CsvSource(
        "Scope",
        "/Scope",
        "/Scope3",
        "/Sco_pe",
        "/ÉScope",
        "/Scope/Subscope",
        "/Scope/Sub_scope",
        "/Scope/Subscope/Subsubscope"
    )
    fun `it should validate valid scope name`(scope: String) = runTest {
        scope.checkScopesNamesAreNgsiLdSupported().shouldSucceed()
    }

    @Test
    fun `it should validate valid scopes names`() = runTest {
        listOf("/A/B", "A/B", "A/B_C").checkScopesNamesAreNgsiLdSupported().shouldSucceed()
    }

    @Test
    fun `it should not validate an entity with an invalid scope name`() = runTest {
        val rawEntity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device",
                "scope": "/3Scope"
            }
            """.trimIndent()

        rawEntity.deserializeAsMap().checkContentIsNgsiLdSupported()
            .shouldFail {
                assertInstanceOf(BadRequestDataException::class.java, it)
                assertEquals(
                    "The JSON-LD object contains a scope with invalid characters (4.18): /3Scope",
                    it.message
                )
            }
    }

    @Test
    fun `it should not validate an entity with invalid scopes names`() = runTest {
        val rawEntity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device",
                "scope": ["/Scope", "/3Scope"]
            }
            """.trimIndent()

        rawEntity.deserializeAsMap().checkContentIsNgsiLdSupported()
            .shouldFail {
                assertInstanceOf(BadRequestDataException::class.java, it)
                assertEquals(
                    "The JSON-LD object contains a scope with invalid characters (4.18): /3Scope",
                    it.message
                )
            }
    }

    @Test
    fun `it should not validate an entity with a null value`() = runTest {
        val rawEntity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device",
                "device": {
                    "type": "Property",
                    "value": null
                }
            }
            """.trimIndent()

        rawEntity.deserializeAsMap().checkContentIsNgsiLdSupported()
            .shouldFail {
                assertInstanceOf(BadRequestDataException::class.java, it)
                assertEquals(
                    "The JSON-LD object contains a member with a null value (5.5.4)",
                    it.message
                )
            }
    }
}
