// package com.egm.stellio.search.entity.compaction
//
// import com.egm.stellio.search.common.config.SearchProperties
// import com.egm.stellio.search.csr.service.ContextSourceRegistrationService
// import com.egm.stellio.search.entity.service.EntityQueryService
// import com.egm.stellio.search.entity.service.EntityService
// import com.egm.stellio.search.entity.service.LinkedEntityService
// import com.egm.stellio.shared.config.ApplicationProperties
// import com.egm.stellio.shared.util.toUri
// import com.ninjasquad.springmockk.MockkBean
// import io.mockk.coEvery
// import org.junit.jupiter.api.BeforeEach
// import org.springframework.beans.factory.annotation.Autowired
// import org.springframework.boot.context.properties.EnableConfigurationProperties
// import org.springframework.test.context.ActiveProfiles
//
// // DO NOT REVIEW WIP
// // DO NOT REVIEW WIP
// // DO NOT REVIEW WIP
// // DO NOT REVIEW WIP
// // DO NOT REVIEW WIP
//
// @ActiveProfiles("test")
// @EnableConfigurationProperties(ApplicationProperties::class, SearchProperties::class)
// class EntityCompactionServiceTests {
//
//    @Autowired
//    private lateinit var applicationProperties: ApplicationProperties
//
//    @MockkBean
//    private lateinit var entitySourceService: EntityCompactionService
//
//    @MockkBean
//    private lateinit var entityService: EntityService
//
//    @MockkBean
//    private lateinit var entityQueryService: EntityQueryService
//
//    @MockkBean
//    private lateinit var contextSourceRegistrationService: ContextSourceRegistrationService
//
//    @MockkBean(relaxed = true)
//    private lateinit var linkedEntityService: LinkedEntityService
//
//    @BeforeEach
//    fun mockCSR() {
//        coEvery {
//            contextSourceRegistrationService
//                .getContextSourceRegistrations(any(), any(), any())
//        } returns listOf()
//    }
//
//    private val beehiveId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
//    private val fishNumberAttribute = "https://ontology.eglobalmark.com/aquac#fishNumber"
//    private val fishSizeAttribute = "https://ontology.eglobalmark.com/aquac#fishSize"
//
// //    // todo in entitySourceServiceTEst ?
// //    @Test
// //    fun `get entity by id should correctly serialize multi-attribute relationship having more than one instance`(){
// //        coEvery { entityQueryService.queryEntity(any(), MOCK_USER_SUB) } returns ExpandedEntity(
// //            mapOf(
// //                "https://uri.etsi.org/ngsi-ld/default-context/managedBy" to
// //                    listOf(
// //                        mapOf(
// //                            JSONLD_TYPE to "https://uri.etsi.org/ngsi-ld/Relationship",
// //                            NGSILD_RELATIONSHIP_OBJECT to mapOf(
// //                                JSONLD_ID to "urn:ngsi-ld:Beekeeper:1229"
// //                            )
// //                        ),
// //                        mapOf(
// //                            JSONLD_TYPE to "https://uri.etsi.org/ngsi-ld/Relationship",
// //                            NGSILD_RELATIONSHIP_OBJECT to mapOf(
// //                                JSONLD_ID to "urn:ngsi-ld:Beekeeper:1230"
// //                            ),
// //                            NGSILD_DATASET_ID_PROPERTY to mapOf(
// //                                JSONLD_ID to "urn:ngsi-ld:Dataset:managedBy:0215"
// //                            )
// //                        )
// //                    ),
// //                JSONLD_ID to "urn:ngsi-ld:Beehive:4567",
// //                JSONLD_TYPE to listOf("Beehive")
// //            )
// //        ).right()
// //
// //        webClient.get()
// //            .uri("/ngsi-ld/v1/entities/$beehiveId")
// //            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
// //            .exchange()
// //            .expectStatus().isOk
// //            .expectBody().json(
// //                """
// //                 {
// //                    "id":"urn:ngsi-ld:Beehive:4567",
// //                    "type": "Beehive",
// //                    "managedBy":[
// //                       {
// //                          "type": "Relationship",
// //                          "object":"urn:ngsi-ld:Beekeeper:1229"
// //                       },
// //                       {
// //                          "type": "Relationship",
// //                          "datasetId":"urn:ngsi-ld:Dataset:managedBy:0215",
// //                          "object":"urn:ngsi-ld:Beekeeper:1230"
// //                       }
// //                    ],
// //                    "@context": "${applicationProperties.contexts.core}"
// //                }
// //                """.trimIndent()
// //            )
// //    }
//
// //    // todo put on EntitySource Test
// //    @Test
// //    fun `get entity by id should correctly filter the asked attributes`() = runTest {
// //        val compactedEntity = """
// //            {
// //                "id": "$beehiveId",
// //                "type": "Beehive",
// //                "attr1": {
// //                    "type": "Property",
// //                    "value": "some value 1"
// //                },
// //                "attr2": {
// //                    "type": "Property",
// //                    "value": "some value 2"
// //                },
// //                "@context" : [
// //                     "http://localhost:8093/jsonld-contexts/apic-compound.jsonld"
// //                ]
// //            }
// //        """.trimIndent().deserializeAsMap()
// //
// //        mockEntitySourceSuccess(compactedEntity)
// //
// //        webClient.get()
// //            .uri("/ngsi-ld/v1/entities/$beehiveId?attrs=attr2")
// //            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
// //            .exchange()
// //            .expectStatus().isOk
// //            .expectBody()
// //            .jsonPath("$.attr1").doesNotExist()
// //            .jsonPath("$.attr2").isNotEmpty
// //    }
// //
// //    // todo move to CompactedServiceTest
// //    @Test
// //    fun `get entity by id should return 404 if the entity has none of the requested attributes`() {
// //        coEvery { entityQueryService.queryEntity(any(), MOCK_USER_SUB) } returns ExpandedEntity(
// //            mapOf(
// //                "@id" to beehiveId.toString(),
// //                "@type" to listOf(BEEHIVE_TYPE)
// //            )
// //        ).right()
// //
// //        val expectedMessage = entityOrAttrsNotFoundMessage(
// //            beehiveId.toString(),
// //            setOf("https://uri.etsi.org/ngsi-ld/default-context/attr2")
// //        )
// //        webClient.get()
// //            .uri("/ngsi-ld/v1/entities/$beehiveId?attrs=attr2")
// //            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
// //            .exchange()
// //            .expectStatus().isNotFound
// //            .expectBody().json(
// //                """
// //                {
// //                    "type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
// //                    "title": "$expectedMessage",
// //                    "detail": "$DEFAULT_DETAIL"
// //                }
// //                """.trimIndent()
// //            )
// //    }
// //
// //    // todo in entitySourceServiceTest and for dateTime
// //    @Test
// //    fun `get entity by id should correctly serialize properties of type DateTime `() {
// //        coEvery { entityQueryService.queryEntity(any(), MOCK_USER_SUB) } returns ExpandedEntity(
// //            mapOf(
// //                NGSILD_CREATED_AT_PROPERTY to
// //                    mapOf(
// //                        "@type" to NGSILD_DATE_TIME_TYPE,
// //                        "@value" to Instant.parse("2015-10-18T11:20:30.000001Z").atZone(ZoneOffset.UTC)
// //                    ),
// //                "https://uri.etsi.org/ngsi-ld/default-context/testedAt" to mapOf(
// //                    "@type" to "https://uri.etsi.org/ngsi-ld/Property",
// //                    NGSILD_PROPERTY_VALUE to mapOf(
// //                        "@type" to NGSILD_DATE_TIME_TYPE,
// //                        "@value" to Instant.parse("2015-10-18T11:20:30.000001Z").atZone(ZoneOffset.UTC)
// //                    ),
// //                    NGSILD_CREATED_AT_PROPERTY to
// //                        mapOf(
// //                            "@type" to NGSILD_DATE_TIME_TYPE,
// //                            "@value" to Instant.parse("2015-10-18T11:20:30.000001Z").atZone(ZoneOffset.UTC)
// //                        ),
// //                    NGSILD_MODIFIED_AT_PROPERTY to
// //                        mapOf(
// //                            "@type" to NGSILD_DATE_TIME_TYPE,
// //                            "@value" to Instant.parse("2015-10-18T12:20:30.000001Z").atZone(ZoneOffset.UTC)
// //                        )
// //                ),
// //                "@id" to beehiveId.toString(),
// //                "@type" to listOf("Beehive")
// //            )
// //        ).right()
// //
// //        webClient.get()
// //            .uri("/ngsi-ld/v1/entities/$beehiveId?options=sysAttrs")
// //            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
// //            .exchange()
// //            .expectStatus().isOk
// //            .expectBody().json(
// //                """
// //                {
// //                    "createdAt":"2015-10-18T11:20:30.000001Z",
// //                    "testedAt":{
// //                        "type": "Property",
// //                        "value":{
// //                            "type": "DateTime",
// //                            "@value":"2015-10-18T11:20:30.000001Z"
// //                        },
// //                        "createdAt":"2015-10-18T11:20:30.000001Z",
// //                        "modifiedAt":"2015-10-18T12:20:30.000001Z"
// //                    },
// //                    "@context": "${applicationProperties.contexts.core}"
// //                }
// //                """.trimIndent()
// //            )
// //    }
// //
// //    // todo in entitySourceServiceTest
// //    @Test
// //    fun `get entity by id should correctly serialize properties of type Date`() {
// //        coEvery { entityQueryService.queryEntity(any(), MOCK_USER_SUB) } returns ExpandedEntity(
// //            mapOf(
// //                "https://uri.etsi.org/ngsi-ld/default-context/testedAt" to mapOf(
// //                    "@type" to "https://uri.etsi.org/ngsi-ld/Property",
// //                    NGSILD_PROPERTY_VALUE to mapOf(
// //                        "@type" to NGSILD_DATE_TYPE,
// //                        "@value" to LocalDate.of(2015, 10, 18)
// //                    )
// //                ),
// //                "@id" to beehiveId.toString(),
// //                "@type" to listOf("Beehive")
// //            )
// //        ).right()
// //
// //        webClient.get()
// //            .uri("/ngsi-ld/v1/entities/$beehiveId")
// //            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
// //            .exchange()
// //            .expectStatus().isOk
// //            .expectBody().json(
// //                """
// //                {
// //                    "testedAt":{
// //                        "type": "Property",
// //                        "value":{
// //                            "type": "Date",
// //                            "@value":"2015-10-18"
// //                        }
// //                    },
// //                    "@context": "${applicationProperties.contexts.core}"
// //                }
// //                """.trimIndent()
// //            )
// //    }
// //
// //    // todo in entitySourceServiceTEst
// //    @Test
// //    fun `get entity by id should correctly serialize properties of type Time`() {
// //        coEvery { entityQueryService.queryEntity(any(), MOCK_USER_SUB) } returns ExpandedEntity(
// //            mapOf(
// //                "https://uri.etsi.org/ngsi-ld/default-context/testedAt" to mapOf(
// //                    "@type" to "https://uri.etsi.org/ngsi-ld/Property",
// //                    NGSILD_PROPERTY_VALUE to mapOf(
// //                        "@type" to NGSILD_TIME_TYPE,
// //                        "@value" to LocalTime.of(11, 20, 30)
// //                    )
// //                ),
// //                "@id" to "urn:ngsi-ld:Beehive:4567",
// //                "@type" to listOf("Beehive")
// //            )
// //        ).right()
// //
// //        webClient.get()
// //            .uri("/ngsi-ld/v1/entities/$beehiveId")
// //            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
// //            .exchange()
// //            .expectStatus().isOk
// //            .expectBody().json(
// //                """
// //                {
// //                    "testedAt":{
// //                        "type": "Property",
// //                        "value":{
// //                            "type": "Time",
// //                            "@value":"11:20:30"
// //                        }
// //                    },
// //                    "@context": "${applicationProperties.contexts.core}"
// //                }
// //                """.trimIndent()
// //            )
// //    }
// //
// //    // todo in entitySourceServiceTEst ?
// //    @Test
// //    fun `get entity by id should correctly serialize multi-attribute property having one instance`() {
// //        coEvery { entityQueryService.queryEntity(any(), MOCK_USER_SUB) } returns ExpandedEntity(
// //            mapOf(
// //                "https://uri.etsi.org/ngsi-ld/default-context/name" to
// //                    mapOf(
// //                        JSONLD_TYPE to "https://uri.etsi.org/ngsi-ld/Property",
// //                        NGSILD_PROPERTY_VALUE to "ruche",
// //                        NGSILD_DATASET_ID_PROPERTY to mapOf(
// //                            JSONLD_ID to "urn:ngsi-ld:Property:french-name"
// //                        )
// //                    ),
// //                JSONLD_ID to "urn:ngsi-ld:Beehive:4567",
// //                JSONLD_TYPE to listOf("Beehive")
// //            )
// //        ).right()
// //
// //        webClient.get()
// //            .uri("/ngsi-ld/v1/entities/$beehiveId")
// //            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
// //            .exchange()
// //            .expectStatus().isOk
// //            .expectBody().json(
// //                """
// //                {
// //                    "id":"urn:ngsi-ld:Beehive:4567",
// //                    "type": "Beehive",
// //                    "name":{"type": "Property","datasetId":"urn:ngsi-ld:Property:french-name","value":"ruche"},
// //                    "@context": "${applicationProperties.contexts.core}"
// //                }
// //                """.trimIndent()
// //            )
// //    }
// //
// //    // todo in entitySourceServiceTEst ?
// //    @Test
// //    fun `get entity by id should correctly serialize multi-attribute property having more than one instance`() {
// //        coEvery { entityQueryService.queryEntity(any(), MOCK_USER_SUB) } returns ExpandedEntity(
// //            mapOf(
// //                "https://uri.etsi.org/ngsi-ld/default-context/name" to
// //                    listOf(
// //                        mapOf(
// //                            JSONLD_TYPE to "https://uri.etsi.org/ngsi-ld/Property",
// //                            NGSILD_PROPERTY_VALUE to "beehive",
// //                            NGSILD_DATASET_ID_PROPERTY to mapOf(
// //                                JSONLD_ID to "urn:ngsi-ld:Property:english-name"
// //                            )
// //                        ),
// //                        mapOf(
// //                            JSONLD_TYPE to "https://uri.etsi.org/ngsi-ld/Property",
// //                            NGSILD_PROPERTY_VALUE to "ruche",
// //                            NGSILD_DATASET_ID_PROPERTY to mapOf(
// //                                JSONLD_ID to "urn:ngsi-ld:Property:french-name"
// //                            )
// //                        )
// //                    ),
// //                JSONLD_ID to "urn:ngsi-ld:Beehive:4567",
// //                JSONLD_TYPE to listOf("Beehive")
// //            )
// //        ).right()
// //
// //        webClient.get()
// //            .uri("/ngsi-ld/v1/entities/$beehiveId")
// //            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
// //            .exchange()
// //            .expectStatus().isOk
// //            .expectBody().json(
// //                """
// //                 {
// //                    "id":"urn:ngsi-ld:Beehive:4567",
// //                    "type": "Beehive",
// //                    "name":[
// //                        {
// //                            "type": "Property","datasetId":"urn:ngsi-ld:Property:english-name","value":"beehive"
// //                        },
// //                        {
// //                            "type": "Property","datasetId":"urn:ngsi-ld:Property:french-name","value":"ruche"
// //                        }
// //                    ],
// //                    "@context": "${applicationProperties.contexts.core}"
// //                }
// //                """.trimIndent()
// //            )
// //    }
// //
// //    // todo in entitySourceServiceTEst ?
// //    @Test
// //    fun `get entity by id should correctly serialize multi-attribute relationship having one instance`() {
// //        coEvery { entityQueryService.queryEntity(any(), MOCK_USER_SUB) } returns ExpandedEntity(
// //            mapOf(
// //                "https://uri.etsi.org/ngsi-ld/default-context/managedBy" to
// //                    mapOf(
// //                        JSONLD_TYPE to "https://uri.etsi.org/ngsi-ld/Relationship",
// //                        NGSILD_RELATIONSHIP_OBJECT to mapOf(
// //                            JSONLD_ID to "urn:ngsi-ld:Beekeeper:1230"
// //                        ),
// //                        NGSILD_DATASET_ID_PROPERTY to mapOf(
// //                            JSONLD_ID to "urn:ngsi-ld:Dataset:managedBy:0215"
// //                        )
// //                    ),
// //                JSONLD_ID to "urn:ngsi-ld:Beehive:4567",
// //                JSONLD_TYPE to listOf("Beehive")
// //            )
// //        ).right()
// //
// //        webClient.get()
// //            .uri("/ngsi-ld/v1/entities/$beehiveId")
// //            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
// //            .exchange()
// //            .expectStatus().isOk
// //            .expectBody().json(
// //                """
// //                {
// //                    "id":"urn:ngsi-ld:Beehive:4567",
// //                    "type": "Beehive",
// //                    "managedBy": {
// //                      "type": "Relationship",
// //                       "datasetId":"urn:ngsi-ld:Dataset:managedBy:0215",
// //                        "object":"urn:ngsi-ld:Beekeeper:1230"
// //                    },
// //                    "@context": "${applicationProperties.contexts.core}"
// //                }
// //                """.trimIndent()
// //            )
// //    }
//
// // DO NOT REVIEW WIP
// //
// //    @Test
// //    fun `get entity by id should return the warnings sent by the CSRs and update the CSRs statuses`() {
// //        val csr = gimmeRawCSR()
// //        coEvery {
// //            entityQueryService.queryEntity("urn:ngsi-ld:BeeHive:TEST".toUri(), sub.getOrNull())
// //        } returns ResourceNotFoundException("no entity").left()
// //
// //        coEvery {
// //            contextSourceRegistrationService
// //                .getContextSourceRegistrations(any(), any(), any())
// //        } returns listOf(csr, csr)
// //
// //        mockkObject(ContextSourceCaller) {
// //            coEvery {
// //                ContextSourceCaller.retrieveContextSourceEntity(any(), any(), any(), any())
// //            } returns MiscellaneousWarning(
// //                "message with\nline\nbreaks",
// //                csr
// //            ).left() andThen
// //                MiscellaneousWarning("message", csr).left()
// //
// //            coEvery { contextSourceRegistrationService.updateContextSourceStatus(any(), any()) } returns Unit
// //            webClient.get()
// //                .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TEST")
// //                .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
// //                .exchange()
// //                .expectStatus().isNotFound
// //                .expectHeader().valueEquals(
// //                    NGSILDWarning.HEADER_NAME,
// //                    "199 urn:ngsi-ld:ContextSourceRegistration:test \"message with line breaks\"",
// //                    "199 urn:ngsi-ld:ContextSourceRegistration:test \"message\""
// //                )
// //
// //            coVerify(exactly = 2) { contextSourceRegistrationService.updateContextSourceStatus(any(), false) }
// //        }
// //    }
// //
// //
// //    @Test
// //    fun `get entities should return the warnings sent by the CSRs and update the CSRs statuses`() {
// //        val csr = gimmeRawCSR()
// //
// //        coEvery {
// //            entityQueryService.queryEntities(any(), sub.getOrNull())
// //        } returns (emptyList<ExpandedEntity>() to 0).right()
// //
// //        coEvery {
// //            contextSourceRegistrationService
// //                .getContextSourceRegistrations(any(), any(), any())
// //        } returns listOf(csr, csr)
// //
// //        mockkObject(ContextSourceCaller) {
// //            coEvery {
// //                ContextSourceCaller.queryContextSourceEntities(any(), any(), any())
// //            } returns MiscellaneousWarning(
// //                "message with\nline\nbreaks",
// //                csr
// //            ).left() andThen
// //                MiscellaneousWarning("message", csr).left()
// //
// //            coEvery { contextSourceRegistrationService.updateContextSourceStatus(any(), any()) } returns Unit
// //            webClient.get()
// //                .uri("/ngsi-ld/v1/entities?type=$BEEHIVE_COMPACT_TYPE&count=true")
// //                .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
// //                .exchange()
// //                .expectStatus().isOk
// //                .expectHeader().valueEquals(
// //                    NGSILDWarning.HEADER_NAME,
// //                    "199 urn:ngsi-ld:ContextSourceRegistration:test \"message with line breaks\"",
// //                    "199 urn:ngsi-ld:ContextSourceRegistration:test \"message\""
// //                ).expectHeader().valueEquals(RESULTS_COUNT_HEADER, "0",)
// //
// //            coVerify(exactly = 2) { contextSourceRegistrationService.updateContextSourceStatus(any(), false) }
// //        }
// //    }
// }
