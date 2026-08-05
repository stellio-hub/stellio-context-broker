package com.egm.stellio.search.service.model

import com.egm.stellio.search.service.registration.model.InputInformationType
import com.egm.stellio.search.service.registration.model.ServiceMode
import com.egm.stellio.search.service.registration.model.ServiceRegistration
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.shouldFailWith
import com.egm.stellio.shared.util.shouldSucceed
import com.egm.stellio.shared.util.shouldSucceedAndResult
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ServiceRegistrationTests {
    @Test
    fun `deserialize should parse a complete service registration`() = runTest {
        val registration = ServiceRegistration.deserialize(
            """
            {
              "id": "urn:ngsi-ld:ServiceRegistration:sr3689",
              "type": "ServiceRegistration",
              "endpoint": "http://localhost:2345/setLight",
              "entities": [{
                "idPattern": "urn:MyLamp.*$",
                "type": "Lamp"
              }],
              "serviceInformation": {
                "name": "setLight",
                "title": "setLight",
                "description": "Set brightness of light, 0 means off",
                "mode": "asynchronous",
                "input": {
                  "type": "object",
                  "required": true,
                  "properties": {
                    "brightness": {
                      "type": "integer",
                      "required": true,
                      "minimum": 0,
                      "maximum": 255
                    }
                  }
                },
                "output": {
                  "type": "string"
                }
              }
            }
            """.trimIndent().deserializeAsMap(),
            emptyList()
        ).shouldSucceedAndResult()

        assertEquals("urn:ngsi-ld:ServiceRegistration:sr3689", registration.id.toString())
        assertEquals("http://localhost:2345/setLight", registration.endpoint.toString())
        assertEquals("urn:MyLamp.*$", registration.entities.single().idPattern)
        assertEquals(listOf("Lamp"), registration.entities.single().types)
        assertEquals("setLight", registration.serviceInformation.name)
        assertEquals("setLight", registration.serviceInformation.title)
        assertEquals(ServiceInformation.ServiceMode.ASYNCHRONOUS, registration.serviceInformation.mode)
        val input = requireNotNull(registration.serviceInformation.input)
        val brightness = requireNotNull(input.properties?.get("brightness"))
        assertEquals(InputInformationType.OBJECT, input.type)
        assertEquals(true, input.required)
        assertEquals(InputInformationType.INTEGER, brightness.type)
        assertEquals(true, brightness.required)
        assertEquals(0.toBigDecimal(), brightness.minimum)
        assertEquals(255.toBigDecimal(), brightness.maximum)
        assertEquals(InputInformationType.STRING, registration.serviceInformation.output?.type)
        registration.validate().shouldSucceed()
    }

    @Test
    fun `deserialize should preserve top-level discovery criteria`() = runTest {
        val registration = ServiceRegistration.deserialize(
            """
            {
              "id": "urn:ngsi-ld:ServiceRegistration:sr3690",
              "type": "ServiceRegistration",
              "endpoint": "http://localhost:2345/setLight",
              "entities": [{
                "type": "Device"
              }],
              "geoQ": {
                "geometry": "Polygon",
                "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]],
                "georel": "within"
              },
              "q": "model==\"modelName\"",
              "scopeQ": "/building/floor1",
              "serviceInformation": {
                "name": "setLight"
              }
            }
            """.trimIndent().deserializeAsMap(),
            emptyList()
        ).shouldSucceedAndResult()

        assertEquals("model==\"modelName\"", registration.q)
        assertEquals("Polygon", registration.geoQ?.geometry)
        assertEquals("within", registration.geoQ?.georel)
        assertEquals("/building/floor1", registration.scopeQ)
        registration.validate().shouldSucceed()
    }

    @Test
    fun `deserialize should reject an unsupported nested service mode`() = runTest {
        ServiceRegistration.deserialize(
            """
            {
              "endpoint": "http://localhost:2345/setLight",
              "entities": [{
                "type": "Lamp"
              }],
              "serviceInformation": {
                "name": "setLight",
                "mode": "deferred"
              }
            }
            """.trimIndent().deserializeAsMap(),
            emptyList()
        ).shouldFailWith {
            it is BadRequestDataException &&
                it.message.startsWith("Service registration cannot be parsed: Cannot deserialize value of type") &&
                it.message.contains("ServiceMode") &&
                it.message.contains("\"deferred\"")
        }
    }

    @Test
    fun `validate should reject an invalid geo query`() = runTest {
        val registration = ServiceRegistration.deserialize(
            """
            {
              "endpoint": "http://localhost:2345/setLight",
              "entities": [{
                "type": "Lamp"
              }],
              "geoQ": {
                "geometry": "InvalidGeometry",
                "coordinates": [0, 0],
                "georel": "within"
              },
              "serviceInformation": {
                "name": "setLight"
              }
            }
            """.trimIndent().deserializeAsMap(),
            emptyList()
        ).shouldSucceedAndResult()

        registration.validate().shouldFailWith {
            it is BadRequestDataException && it.message.contains("not a recognized value for 'geometry'")
        }
    }

    @Test
    fun `mergeWithFragment should update the endpoint and modification date`() = runTest {
        val registration = ServiceRegistration.deserialize(
            """
            {
              "endpoint": "http://localhost:2345/setLight",
              "entities": [{
                "type": "Lamp"
              }],
              "serviceInformation": {
                "name": "setLight"
              }
            }
            """.trimIndent().deserializeAsMap(),
            emptyList()
        ).shouldSucceedAndResult()

        val updated = registration.mergeWithFragment(
            mapOf("endpoint" to "http://localhost:4567/setLight"),
            emptyList()
        ).shouldSucceedAndResult()

        assertEquals("http://localhost:4567/setLight", updated.endpoint.toString())
        assertThat(updated.modifiedAt).isAfterOrEqualTo(registration.modifiedAt)
    }
}
