package com.egm.datahub.context.registry.service

import com.egm.datahub.context.registry.model.GeoProperty
import com.egm.datahub.context.registry.model.Observation
import com.egm.datahub.context.registry.model.ObservedBy
import com.egm.datahub.context.registry.model.Value
import com.egm.datahub.context.registry.repository.Neo4jRepository
import com.ninjasquad.springmockk.MockkBean
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockkClass
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.neo4j.ogm.response.model.NodeModel
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.time.OffsetDateTime

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [ Neo4jService::class ])
@ActiveProfiles("test")
class Neo4jServiceTests {

    @Autowired
    private lateinit var neo4jService: Neo4jService

    @MockkBean
    private lateinit var neo4jRepository: Neo4jRepository

    @Test
    fun `it should ignore measures from an unknown sensor`() {
        val observation = gimmeAnObservation()

        every { neo4jRepository.getNodeByURI(any()) } returns emptyMap()

        neo4jService.updateEntityLastMeasure(observation)

        verify { neo4jRepository.getNodeByURI("urn:ngsi-ld:Sensor:10e2073a01080065") }

        confirmVerified(neo4jRepository)
    }

    @Test
    fun `it should create a new measure`() {
        val observation = gimmeAnObservation()

        val mockkedNodeModel = mockkClass(NodeModel::class)

        every { neo4jRepository.getNodeByURI(any()) } returns mapOf("n" to mockkedNodeModel)
        every { neo4jRepository.getEntitiesByLabelAndQuery(any(), any()) } returns mutableListOf()
        every { mockkedNodeModel.property("uri") } returns "urn:ngsi-ld:Sensor:10e2073a01080065"
        every { mockkedNodeModel.labels } returns arrayOf("Sensor")
        every { neo4jRepository.createEntity(any(), any(), any()) } returns "urn:ngsi-ld:Measure:12345678909876"

        neo4jService.updateEntityLastMeasure(observation)

        verify { neo4jRepository.getNodeByURI("urn:ngsi-ld:Sensor:10e2073a01080065") }
        verify { neo4jRepository.getEntitiesByLabelAndQuery(
            listOf("observedBy==urn:ngsi-ld:Sensor:10e2073a01080065", "unitCode==CEL"), "Measure")
        }
        verify { neo4jRepository.createEntity("urn:ngsi-ld:Measure:12345678909876", match { it.size == 1 }, match { it.size == 1 }) }

        confirmVerified()
    }

    @Test
    fun `it should update an existing measure`() {
        val observation = gimmeAnObservation()

        val mockkedSensorNode = mockkClass(NodeModel::class)
        val mockkedMeasureNode = mockkClass(NodeModel::class)

        every { neo4jRepository.getNodeByURI(any()) } returns mapOf("n" to mockkedSensorNode)
        every { neo4jRepository.getEntitiesByLabelAndQuery(any(), any()) } returns mutableListOf(mapOf("n" to mockkedMeasureNode))
        every { mockkedMeasureNode.property("uri") } returns "urn:ngsi-ld:Measure:12345678909876"

        every { neo4jRepository.updateEntity(any(), any()) } returns emptyMap()

        neo4jService.updateEntityLastMeasure(observation)

        verify { neo4jRepository.getNodeByURI("urn:ngsi-ld:Sensor:10e2073a01080065") }
        verify { neo4jRepository.getEntitiesByLabelAndQuery(
            listOf("observedBy==urn:ngsi-ld:Sensor:10e2073a01080065", "unitCode==CEL"), "Measure")
        }
        verify { neo4jRepository.updateEntity(any(), "urn:ngsi-ld:Measure:12345678909876") }

        confirmVerified()
    }

    private fun gimmeAnObservation(): Observation {
        return Observation(id = "urn:ngsi-ld:Measure:12345678909876", type = "Measure",
            observedBy = ObservedBy(type = "Relationship", target = "urn:ngsi-ld:Sensor:10e2073a01080065"),
            value = 20.4, unitCode = "CEL", observedAt = OffsetDateTime.now(),
            location = GeoProperty(type = "GeoProperty", value = Value(type = "Point", coordinates = listOf(43.43, 54.54))))
    }
}
