package com.egm.stellio.subscription.service

import arrow.core.filterIsInstance
import arrow.core.right
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_COMPACTED_ENTITY_CORE_MEMBERS
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LANG_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_TERM
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.web.NGSILD_TENANT_HEADER
import com.egm.stellio.subscription.model.Endpoint
import com.egm.stellio.subscription.model.EndpointInfo
import com.egm.stellio.subscription.model.NotificationParams
import com.egm.stellio.subscription.model.NotificationParams.FormatType
import com.egm.stellio.subscription.model.NotificationTrigger.*
import com.egm.stellio.subscription.service.mqtt.MQTTNotificationService
import com.egm.stellio.subscription.support.gimmeRawSubscription
import com.github.tomakehurst.wiremock.client.WireMock.*
import com.github.tomakehurst.wiremock.junit5.WireMockTest
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.confirmVerified
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.mono
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpHeaders
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [NotificationService::class])
@WireMockTest(httpPort = 8089)
@ActiveProfiles("test")
class NotificationServiceTests {

    @MockkBean
    private lateinit var subscriptionService: SubscriptionService

    @MockkBean
    private lateinit var mqttNotificationService: MQTTNotificationService

    @Autowired
    private lateinit var notificationService: NotificationService

    private val apiaryId = "urn:ngsi-ld:Apiary:XYZ01"

    private val rawEntity =
        """
        {
           "id":"$apiaryId",
           "type":"Apiary",
           "name":{
              "type":"Property",
              "value":"ApiarySophia"
           },
           "managedBy":{
              "type":"Relationship",
              "object":"urn:ngsi-ld:Beekeeper:01"
           },
           "location": {
              "type": "GeoProperty",
              "value": {
                 "type": "Point",
                 "coordinates": [
                    24.30623,
                    60.07966
                 ]
              }
           },
           "@context":[ "$APIC_COMPOUND_CONTEXT" ]
        } 
        """.trimIndent()

    private val entityWithSysAttrs =
        """
        {
           "id":"$apiaryId",
           "type":"Apiary",
           "createdAt": "2024-02-13T18:15:00Z",
           "modifiedAt": "2024-02-13T18:16:00Z",
           "name": {
              "type":"Property",
              "value":"ApiarySophia",
              "createdAt": "2024-02-13T18:15:00Z",
              "modifiedAt": "2024-02-13T18:16:00Z"
           },
           "@context":[ "$APIC_COMPOUND_CONTEXT" ]
        }
        """.trimIndent()

    @Test
    fun `it should notify the subscriber and update the subscription`() = runTest {
        val subscription = gimmeRawSubscription()
        val expandedEntity = expandJsonLdEntity(rawEntity)

        coEvery {
            subscriptionService.getMatchingSubscriptions(any(), any(), any())
        } returns listOf(subscription).right()
        coEvery { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } returns 1

        stubFor(
            post(urlMatching("/notification"))
                .willReturn(ok())
        )

        notificationService.notifyMatchingSubscribers(
            expandedEntity,
            setOf(NGSILD_NAME_PROPERTY),
            ATTRIBUTE_UPDATED
        ).shouldSucceedWith {
            assertEquals(1, it.size)
            assertEquals(subscription.id, it[0].second.subscriptionId)
            assertEquals(1, it[0].second.data.size)
            assertTrue(it[0].third)
        }

        coVerify {
            subscriptionService.getMatchingSubscriptions(
                expandedEntity,
                setOf(NGSILD_NAME_PROPERTY),
                ATTRIBUTE_UPDATED
            )
        }
        coVerify { subscriptionService.updateSubscriptionNotification(any(), any(), any()) }
        confirmVerified(subscriptionService)
    }

    @Test
    fun `it should notify the subscriber and only keep the expected attributes`() = runTest {
        val subscription = gimmeRawSubscription(
            withNotifParams = Pair(
                FormatType.NORMALIZED,
                listOf(NGSILD_NAME_PROPERTY, NGSILD_LOCATION_PROPERTY)
            ),
            contexts = APIC_COMPOUND_CONTEXTS
        )
        val expandedEntity = expandJsonLdEntity(rawEntity)

        coEvery {
            subscriptionService.getMatchingSubscriptions(any(), any(), any())
        } returns listOf(subscription).right()
        coEvery { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } returns 1

        stubFor(
            post(urlMatching("/notification"))
                .willReturn(ok())
        )

        notificationService.notifyMatchingSubscribers(
            expandedEntity,
            setOf(NGSILD_NAME_PROPERTY),
            ATTRIBUTE_UPDATED
        ).shouldSucceedWith {
            assertEquals(1, it.size)
            assertEquals(subscription.id, it[0].second.subscriptionId)
            assertEquals(1, it[0].second.data.size)
            assertEquals(5, it[0].second.data[0].size)
            assertTrue(
                it[0].second.data[0].all { entry ->
                    JSONLD_COMPACTED_ENTITY_CORE_MEMBERS
                        .plus(NGSILD_NAME_TERM)
                        .plus(NGSILD_LOCATION_TERM)
                        .contains(entry.key)
                }
            )
            assertTrue(it[0].third)
        }
    }

    @Test
    fun `it should notify the subscriber and use the contexts of the subscription to compact`() = runTest {
        val subscription = gimmeRawSubscription().copy(
            notification = NotificationParams(
                attributes = emptyList(),
                endpoint = Endpoint(
                    uri = "http://localhost:8089/notification".toUri(),
                    accept = Endpoint.AcceptType.JSONLD
                )
            ),
            contexts = listOf(NGSILD_TEST_CORE_CONTEXT)
        )
        val expandedEntity = expandJsonLdEntity(rawEntity)

        coEvery {
            subscriptionService.getMatchingSubscriptions(any(), any(), any())
        } returns listOf(subscription).right()
        coEvery { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } returns 1

        stubFor(
            post(urlMatching("/notification"))
                .willReturn(ok())
        )

        notificationService.notifyMatchingSubscribers(
            expandedEntity,
            setOf(NGSILD_NAME_PROPERTY),
            ATTRIBUTE_UPDATED
        ).shouldSucceedWith { notificationResults ->
            val notificationResult = notificationResults[0]
            assertEquals(subscription.id, notificationResult.first.id)
            assertEquals(subscription.id, notificationResult.second.subscriptionId)
            assertEquals(1, notificationResult.second.data.size)
            assertTrue(notificationResult.second.data[0].containsKey(NGSILD_NAME_PROPERTY))
            assertTrue(notificationResult.second.data[0].containsKey(MANAGED_BY_RELATIONSHIP))
            assertEquals(NGSILD_TEST_CORE_CONTEXT, notificationResult.second.data[0][JsonLdUtils.JSONLD_CONTEXT])
            assertTrue(notificationResult.third)
        }
    }

    @Test
    fun `it should send a simplified payload when format is keyValues and include only the specified attributes`() =
        runTest {
            val subscription = gimmeRawSubscription(
                withNotifParams = Pair(FormatType.KEY_VALUES, listOf(NGSILD_LOCATION_TERM))
            )
            val expandedEntity = expandJsonLdEntity(rawEntity)

            coEvery {
                subscriptionService.getMatchingSubscriptions(any(), any(), any())
            } returns listOf(subscription).right()
            coEvery { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } returns 1

            stubFor(
                post(urlMatching("/notification"))
                    .willReturn(ok())
            )

            notificationService.notifyMatchingSubscribers(
                expandedEntity,
                setOf(NGSILD_NAME_PROPERTY),
                ATTRIBUTE_UPDATED
            ).shouldSucceedWith {
                assertEquals(1, it.size)
                assertEquals(subscription.id, it[0].second.subscriptionId)
                assertEquals(1, it[0].second.data.size)
                assertTrue(it[0].third)
            }

            coVerify {
                subscriptionService.getMatchingSubscriptions(
                    expandedEntity,
                    setOf(NGSILD_NAME_PROPERTY),
                    ATTRIBUTE_UPDATED
                )
            }
            coVerify { subscriptionService.updateSubscriptionNotification(any(), any(), any()) }
            confirmVerified(subscriptionService)
        }

    @Test
    fun `it should notify the two subscribers`() = runTest {
        val subscription1 = gimmeRawSubscription()
        val subscription2 = gimmeRawSubscription()
        val expandedEntity = expandJsonLdEntity(rawEntity)

        coEvery {
            subscriptionService.getMatchingSubscriptions(any(), any(), any())
        } returns listOf(subscription1, subscription2).right()
        coEvery { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } returns 1

        stubFor(
            post(urlMatching("/notification"))
                .willReturn(ok())
        )

        notificationService.notifyMatchingSubscribers(
            expandedEntity,
            setOf(NGSILD_NAME_PROPERTY),
            ATTRIBUTE_DELETED
        ).shouldSucceedWith {
            assertEquals(2, it.size)
        }

        coVerify {
            subscriptionService.getMatchingSubscriptions(
                expandedEntity,
                setOf(NGSILD_NAME_PROPERTY),
                ATTRIBUTE_DELETED
            )
        }
        coVerify(exactly = 2) { subscriptionService.updateSubscriptionNotification(any(), any(), any()) }
        confirmVerified(subscriptionService)
    }

    @Test
    fun `it should notify the second subscriber even if the first notification has failed`() = runTest {
        // the notification for this one is expected to fail as we don't listen on port 8088
        val subscription1 = gimmeRawSubscription()
            .copy(
                notification = NotificationParams(
                    attributes = emptyList(),
                    format = FormatType.KEY_VALUES,
                    endpoint = Endpoint(
                        uri = "http://localhost:8088/notification".toUri(),
                        accept = Endpoint.AcceptType.JSONLD
                    )
                )
            )
        val subscription2 = gimmeRawSubscription()
        val expandedEntity = expandJsonLdEntity(rawEntity)

        coEvery {
            subscriptionService.getMatchingSubscriptions(any(), any(), any())
        } returns listOf(subscription1, subscription2).right()
        coEvery { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } returns 1

        stubFor(
            post(urlMatching("/notification"))
                .willReturn(ok())
        )

        notificationService.notifyMatchingSubscribers(
            expandedEntity,
            setOf(NGSILD_NAME_PROPERTY),
            ATTRIBUTE_CREATED
        ).shouldSucceedWith { results ->
            assertEquals(2, results.size)
            assertTrue(
                results.all {
                    it.first.id == subscription1.id && !it.third ||
                        it.first.id == subscription2.id && it.third
                }
            )
        }

        coVerify {
            subscriptionService.getMatchingSubscriptions(
                expandedEntity,
                setOf(NGSILD_NAME_PROPERTY),
                ATTRIBUTE_CREATED
            )
        }
        coVerify(exactly = 2) { subscriptionService.updateSubscriptionNotification(any(), any(), any()) }
        confirmVerified(subscriptionService)
    }

    @Test
    fun `it should add a Link header containing the context of the subscription`() = runTest {
        val subscription = gimmeRawSubscription().copy(
            notification = NotificationParams(
                attributes = emptyList(),
                endpoint = Endpoint(
                    uri = "http://localhost:8089/notification".toUri(),
                    accept = Endpoint.AcceptType.JSON
                )
            )
        )

        coEvery { subscriptionService.getContextsLink(any()) } returns buildContextLinkHeader(NGSILD_TEST_CORE_CONTEXT)
        coEvery { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } returns 1

        stubFor(
            post(urlMatching("/notification"))
                .willReturn(ok())
        )

        notificationService.callSubscriber(subscription, rawEntity.deserializeAsMap())

        val link = buildContextLinkHeader(subscription.contexts[0])
        verify(
            1,
            postRequestedFor(urlPathEqualTo("/notification"))
                .withHeader(HttpHeaders.LINK, equalTo(link))
        )
    }

    @Test
    fun `it should add an NGSILD-Tenant header if the subscription is not from the default context`() = runTest {
        val subscription = gimmeRawSubscription().copy(
            notification = NotificationParams(
                attributes = emptyList(),
                endpoint = Endpoint(
                    uri = "http://localhost:8089/notification".toUri(),
                    accept = Endpoint.AcceptType.JSONLD
                )
            )
        )

        coEvery { subscriptionService.getContextsLink(any()) } returns buildContextLinkHeader(NGSILD_TEST_CORE_CONTEXT)
        coEvery { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } returns 1

        stubFor(
            post(urlMatching("/notification"))
                .willReturn(ok())
        )

        mono {
            notificationService.callSubscriber(subscription, rawEntity.deserializeAsMap())
        }.contextWrite { context ->
            context.put(NGSILD_TENANT_HEADER, "urn:ngsi-ld:tenant:01")
        }.awaitSingle()

        verify(
            1,
            postRequestedFor(urlPathEqualTo("/notification"))
                .withHeader(NGSILD_TENANT_HEADER, equalTo("urn:ngsi-ld:tenant:01"))
        )
    }

    @Test
    fun `it should call the subscriber`() = runTest {
        val subscription = gimmeRawSubscription()

        coEvery { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } returns 1

        stubFor(
            post(urlMatching("/notification"))
                .willReturn(ok())
        )

        val notificationResult = notificationService.callSubscriber(
            subscription,
            rawEntity.deserializeAsMap()
        )

        assertEquals(subscription.id, notificationResult.first.id)
        assertEquals(subscription.id, notificationResult.second.subscriptionId)
        assertEquals(1, notificationResult.second.data.size)
        assertTrue(notificationResult.third)

        coVerify(exactly = 1) { subscriptionService.updateSubscriptionNotification(subscription, any(), true) }
        confirmVerified(subscriptionService)

        verify(1, postRequestedFor(urlPathEqualTo("/notification")))
    }

    @Test
    fun `it should notify the subscriber and return entities without sysAttrs if sysAttrs is false`() = runTest {
        val subscription = gimmeRawSubscription()
        val expandedEntity = expandJsonLdEntity(entityWithSysAttrs)

        coEvery {
            subscriptionService.getMatchingSubscriptions(any(), any(), any())
        } returns listOf(subscription).right()
        coEvery { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } returns 1

        stubFor(
            post(urlMatching("/notification"))
                .willReturn(ok())
        )

        notificationService.notifyMatchingSubscribers(
            expandedEntity,
            setOf(NGSILD_NAME_PROPERTY),
            ATTRIBUTE_UPDATED
        ).shouldSucceedWith {
            val entity = it[0].second.data[0]
            assertFalse(entity.containsKey("createdAt"))
            assertFalse(entity.containsKey("modifiedAt"))

            entity.filterIsInstance<String, Map<String, Any>>()
                .forEach { (_, v) ->
                    assertThat(v).doesNotContainKey("createdAt")
                    assertThat(v).doesNotContainKey("modifiedAt")
                }
        }
    }

    @Test
    fun `it should notify the subscriber with sysAttrs if sysAttrs is true`() = runTest {
        val subscription = gimmeRawSubscription().copy(
            notification = NotificationParams(
                attributes = emptyList(),
                endpoint = Endpoint(
                    uri = "http://localhost:8089/notification".toUri(),
                    accept = Endpoint.AcceptType.JSONLD
                ),
                sysAttrs = true
            )
        )

        val expandedEntity = expandJsonLdEntity(entityWithSysAttrs)

        coEvery {
            subscriptionService.getMatchingSubscriptions(any(), any(), any())
        } returns listOf(subscription).right()
        coEvery { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } returns 1

        stubFor(
            post(urlMatching("/notification"))
                .willReturn(ok())
        )

        notificationService.notifyMatchingSubscribers(
            expandedEntity,
            setOf(NGSILD_NAME_PROPERTY),
            ATTRIBUTE_UPDATED
        ).shouldSucceedWith {
            val entity = it[0].second.data[0]
            assertTrue(entity.containsKey("createdAt"))
            assertTrue(entity.containsKey("modifiedAt"))

            entity.filterIsInstance<String, Map<String, Any>>()
                .forEach { (_, v) ->
                    assertThat(v).containsKey("createdAt")
                    assertThat(v).containsKey("modifiedAt")
                }
        }
    }

    @Test
    fun `it should notify the subscriber with language filter applied if lang is provided`() = runTest {
        val subscription = gimmeRawSubscription().copy(
            notification = NotificationParams(
                attributes = emptyList(),
                endpoint = Endpoint(
                    uri = "http://localhost:8089/notification".toUri(),
                    accept = Endpoint.AcceptType.JSONLD
                )
            ),
            lang = "fr",
            contexts = APIC_COMPOUND_CONTEXTS
        )

        val expandedEntity = expandJsonLdEntity(
            """
            {
               "id":"$apiaryId",
               "type":"Apiary",
               "friendlyName": {
                  "type":"LanguageProperty",
                  "languageMap": {
                    "fr": "Le rucher de Nantes",
                    "en": "The apiary of Nantes"
                  }
               },
               "@context":[ "$APIC_COMPOUND_CONTEXT" ]
            }
            """.trimIndent()
        )

        coEvery {
            subscriptionService.getMatchingSubscriptions(any(), any(), any())
        } returns listOf(subscription).right()
        coEvery { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } returns 1

        stubFor(
            post(urlMatching("/notification"))
                .willReturn(ok())
        )

        notificationService.notifyMatchingSubscribers(
            expandedEntity,
            setOf(FRIENDLYNAME_LANGUAGEPROPERTY),
            ATTRIBUTE_UPDATED
        ).shouldSucceedWith {
            val entity = it[0].second.data[0]
            entity.filterKeys { key -> key == FRIENDLYNAME_COMPACT_LANGUAGEPROPERTY }
                .also { property ->
                    val propertyValues = property[FRIENDLYNAME_COMPACT_LANGUAGEPROPERTY] as Map<String, Any>
                    assertThat(propertyValues)
                        .containsEntry(JSONLD_TYPE_TERM, NGSILD_PROPERTY_TERM)
                        .containsEntry(JSONLD_VALUE_TERM, "Le rucher de Nantes")
                        .containsEntry(NGSILD_LANG_TERM, "fr")
                }
        }
    }

    @Test
    fun `callSuscriber should ask mqttNotifier if the brokerUrl startWith mqtt`() = runTest {
        val subscription = gimmeRawSubscription().copy(
            notification = NotificationParams(
                attributes = emptyList(),
                endpoint = Endpoint(
                    uri = "mqtt://localhost:8089/notification".toUri(),
                    accept = Endpoint.AcceptType.JSON
                )
            )
        )

        coEvery { subscriptionService.getContextsLink(any()) } returns buildContextLinkHeader(NGSILD_TEST_CORE_CONTEXT)
        coEvery { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } returns 1

        coEvery { mqttNotificationService.mqttNotifier(any(), any(), any()) } returns true

        notificationService.callSubscriber(subscription, rawEntity.deserializeAsMap())

        coVerify(exactly = 1) { mqttNotificationService.mqttNotifier(any(), any(), any()) }
    }

    @Test
    fun `callSuscriber should generate headers for mqtt notification`() = runTest {
        val infoKey = "hello"
        val infoValue = "world"
        val subscription = gimmeRawSubscription().copy(
            notification = NotificationParams(
                attributes = emptyList(),
                endpoint = Endpoint(
                    uri = "mqtt://localhost:8089/notification".toUri(),
                    accept = Endpoint.AcceptType.JSON,
                    receiverInfo = listOf(EndpointInfo(key = infoKey, value = infoValue)),
                )
            )
        )

        coEvery { mqttNotificationService.mqttNotifier(any(), any(), any()) } returns true

        coEvery { subscriptionService.getContextsLink(any()) } returns buildContextLinkHeader(NGSILD_TEST_CORE_CONTEXT)
        coEvery { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } returns 1

        notificationService.callSubscriber(subscription, rawEntity.deserializeAsMap())

        coVerify {
            mqttNotificationService.mqttNotifier(
                any(),
                any(),
                match {
                    it["Content-Type"] == "application/json" &&
                        it[infoKey] == infoValue &&
                        it["Link"] != null
                }
            )
        }
    }
}
