package com.egm.stellio.entity.util

import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.params.provider.Arguments
import java.net.URI
import java.util.stream.Stream

@Suppress("unused")
class QueryEntitiesParameterizedTests private constructor() {

    companion object {
        @JvmStatic
        fun rawResultsProvider(): Stream<Arguments> {
            return Stream.of(
                Arguments.arguments(
                    null,
                    0,
                    50,
                    listOf(
                        "urn:ngsi-ld:Beekeeper:01231".toUri(),
                        "urn:ngsi-ld:Beekeeper:01232".toUri(),
                        "urn:ngsi-ld:Beekeeper:03432".toUri()
                    )
                ),
                Arguments.arguments(
                    null,
                    0,
                    2,
                    listOf(
                        "urn:ngsi-ld:Beekeeper:01231".toUri(),
                        "urn:ngsi-ld:Beekeeper:01232".toUri()
                    )
                ),
                Arguments.arguments(
                    null,
                    2,
                    2,
                    listOf("urn:ngsi-ld:Beekeeper:03432".toUri())
                ),
                Arguments.arguments(
                    "^urn:ngsi-ld:Beekeeper:0.*2$",
                    3,
                    1,
                    emptyList<URI>()
                ),
                Arguments.arguments(
                    "^urn:ngsi-ld:Beekeeper:0.*2$",
                    0,
                    1,
                    listOf("urn:ngsi-ld:Beekeeper:01232".toUri())
                ),
                Arguments.arguments(
                    "^urn:ngsi-ld:Beekeeper:0.*2$",
                    1,
                    1,
                    listOf("urn:ngsi-ld:Beekeeper:03432".toUri())
                ),
                Arguments.arguments(
                    null,
                    1,
                    0,
                    emptyList<URI>()
                )
            )
        }
    }
}
