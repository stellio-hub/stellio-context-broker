package com.egm.datahub.context.search.listener

import com.egm.datahub.context.search.service.ObservationService
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles
import org.apache.kafka.common.record.TimestampType
import org.springframework.core.io.ClassPathResource
import reactor.core.publisher.Mono
import java.time.format.DateTimeFormatter

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [ ObservationListener::class ])
@EmbeddedKafka(topics = ["entities"])
@ActiveProfiles("test")

class ObservationListenerTest {
    val topic = "topic"
    @Autowired
    private lateinit var observationListener: ObservationListener

    @MockkBean
    private lateinit var observationService: ObservationService

    companion object {
        init {
            System.setProperty(EmbeddedKafkaBroker.BROKER_LIST_PROPERTY, "spring.kafka.bootstrap-servers")
        }
    }

    @Test
    fun `it should not raise an exception with a correct ngsild observation`() {

        every { observationService.create(any()) } returns Mono.just(1)

        val jsonLdObservation = ClassPathResource("/ngsild/observation.jsonld")
        observationListener.processMessage(ConsumerRecord(topic, 1, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 1 as Any, String(jsonLdObservation.inputStream.readAllBytes()) as Any))

        verify {
            observationService.create(match { ngsiLdObservation ->
                ngsiLdObservation.id == "urn:sosa:Observation:111122223333" &&
                ngsiLdObservation.type == "Observation" &&
                ngsiLdObservation.unitCode == "CEL" &&
                ngsiLdObservation.value == 20.7 &&
                ngsiLdObservation.observedAt.format(DateTimeFormatter.ISO_INSTANT) == "2019-10-18T07:31:39.770Z" &&
                ngsiLdObservation.observedBy.type == "Relationship" &&
                ngsiLdObservation.observedBy.target == "urn:sosa:Sensor:10e2073a01080065" &&
                ngsiLdObservation.location?.type == "GeoProperty" &&
                ngsiLdObservation.location?.value?.type == "Point" &&
                ngsiLdObservation.location?.value?.coordinates == listOf(24.30623, 60.07966)
            })
        }

        confirmVerified(observationService)
    }

    @Test
    fun `it should raise an exception and ignores the observation`() {

        val jsonLdObservation = ClassPathResource("/ngsild/observationWithoutUnitCode.jsonld")
        observationListener.processMessage(ConsumerRecord(topic, 1, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 1 as Any, String(jsonLdObservation.inputStream.readAllBytes()) as Any))

        verify { observationService wasNot Called }
    }

    @Test
    fun `it should accept the message withtout location because location may be null`() {

        every { observationService.create(any()) } returns Mono.just(1)

        val jsonLdObservation = ClassPathResource("/ngsild/observationWithoutLocation.jsonld")
        observationListener.processMessage(ConsumerRecord(topic, 1, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 1 as Any, String(jsonLdObservation.inputStream.readAllBytes()) as Any))

        verify {
            observationService.create(match { ngsiLdObservation ->
                ngsiLdObservation.id == "urn:sosa:Observation:111122223333" &&
                ngsiLdObservation.type == "Observation" &&
                ngsiLdObservation.unitCode == "CEL" &&
                ngsiLdObservation.value == 20.7 &&
                ngsiLdObservation.observedAt.format(DateTimeFormatter.ISO_INSTANT) == "2019-10-18T07:31:39.770Z" &&
                ngsiLdObservation.observedBy.type == "Relationship" &&
                ngsiLdObservation.observedBy.target == "urn:sosa:Sensor:10e2073a01080065" &&
                ngsiLdObservation.location?.type == null
            })
        }
        confirmVerified(observationService)
    }
}