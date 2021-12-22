package com.egm.stellio.entity.repository

import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.EGM_ROLES
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.R_CAN_ADMIN
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.R_CAN_READ
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.R_CAN_WRITE
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.R_IS_MEMBER_OF
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.SERVICE_ACCOUNT_ID
import com.egm.stellio.entity.authorization.Neo4jAuthorizationService
import com.egm.stellio.entity.config.WithNeo4jContainer
import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.support.WithKafkaContainer
import com.egm.stellio.shared.util.DEFAULT_CONTEXTS
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdKey
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import junit.framework.TestCase.*
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import java.net.URI

@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = ["application.authentication.enabled=true"])
class Neo4jSearchRepositoryTests : WithNeo4jContainer, WithKafkaContainer {

    @Autowired
    private lateinit var searchRepository: SearchRepository

    @Autowired
    private lateinit var neo4jRepository: Neo4jRepository

    @Autowired
    private lateinit var entityRepository: EntityRepository

    @MockkBean(relaxed = true)
    private lateinit var neo4jAuthorizationService: Neo4jAuthorizationService

    private val beekeeperUri = "urn:ngsi-ld:Beekeeper:1230".toUri()
    private val groupUri = "urn:ngsi-ld:Group:01".toUri()
    private val userUri = "urn:ngsi-ld:User:01".toUri()
    private val clientUri = "urn:ngsi-ld:Client:01".toUri()
    private val serviceAccountUri = "urn:ngsi-ld:User:01".toUri()
    private val expandedNameProperty = expandJsonLdKey("name", DEFAULT_CONTEXTS)!!
    private val sub = "01"
    private val offset = 0
    private val limit = 20

    @AfterEach
    fun cleanData() {
        entityRepository.deleteAll()
    }

    @Test
    fun `it should return matching entities that user can access`() {
        val userEntity = createEntity(userUri, listOf(AuthorizationService.USER_LABEL), mutableListOf())
        val firstEntity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:1231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )
        val thirdEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:1232".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )
        createRelationship(EntitySubjectNode(userEntity.id), R_CAN_WRITE, firstEntity.id)
        createRelationship(EntitySubjectNode(userEntity.id), R_CAN_ADMIN, secondEntity.id)
        createRelationship(EntitySubjectNode(userEntity.id), R_CAN_READ, thirdEntity.id)

        val entities = searchRepository.getEntities(
            QueryParams(expandedType = "Beekeeper", q = "name==\"Scalpa\""),
            sub,
            offset,
            limit,
            DEFAULT_CONTEXTS
        ).second

        assertTrue(entities.containsAll(listOf(firstEntity.id, secondEntity.id, thirdEntity.id)))
    }

    @Test
    fun `it should return matching entities that user can access by it's group`() {
        val userEntity = createEntity(userUri, listOf(AuthorizationService.USER_LABEL), mutableListOf())
        val groupEntity = createEntity(groupUri, listOf("Group"), mutableListOf())
        createRelationship(EntitySubjectNode(userEntity.id), R_IS_MEMBER_OF, groupEntity.id)
        val firstEntity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:1231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )
        createRelationship(EntitySubjectNode(groupEntity.id), R_CAN_WRITE, firstEntity.id)
        createRelationship(EntitySubjectNode(groupEntity.id), R_CAN_WRITE, secondEntity.id)

        val entities = searchRepository.getEntities(
            QueryParams(expandedType = "Beekeeper", q = "name==\"Scalpa\""),
            sub,
            offset,
            limit,
            DEFAULT_CONTEXTS
        ).second

        assertTrue(entities.containsAll(listOf(firstEntity.id, secondEntity.id)))
    }

    @Test
    fun `it should not return a matching entity that user cannot access`() {
        createEntity(userUri, listOf(AuthorizationService.USER_LABEL), mutableListOf())
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )
        val entities = searchRepository.getEntities(
            QueryParams(expandedType = "Beekeeper", q = "name==\"Scalpa\""),
            sub,
            offset,
            limit,
            DEFAULT_CONTEXTS
        ).second

        assertFalse(entities.contains(entity.id))
    }

    @Test
    fun `it should return matching entities that client can access`() {
        val clientEntity = createEntity(
            clientUri,
            listOf(AuthorizationService.CLIENT_LABEL),
            mutableListOf(
                Property(
                    name = SERVICE_ACCOUNT_ID,
                    value = serviceAccountUri
                )
            )
        )
        val firstEntity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:1231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )
        createRelationship(EntitySubjectNode(clientEntity.id), R_CAN_READ, firstEntity.id)
        createRelationship(EntitySubjectNode(clientEntity.id), R_CAN_READ, secondEntity.id)

        val queryParams = QueryParams(expandedType = "Beekeeper", q = "name==\"Scalpa\"")
        var entities = searchRepository.getEntities(
            queryParams,
            sub,
            offset,
            limit,
            DEFAULT_CONTEXTS
        ).second

        assertTrue(entities.containsAll(listOf(firstEntity.id, secondEntity.id)))

        entities = searchRepository.getEntities(
            queryParams.copy(expandedAttrs = setOf(expandedNameProperty)),
            sub,
            offset,
            limit,
            DEFAULT_CONTEXTS
        ).second

        assertTrue(entities.containsAll(listOf(firstEntity.id, secondEntity.id)))
    }

    @Test
    fun `it should return all matching entities for admin users`() {
        createEntity(
            userUri,
            listOf(AuthorizationService.USER_LABEL),
            mutableListOf(
                Property(
                    name = EGM_ROLES,
                    value = "admin"
                )
            )
        )
        val firstEntity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:1231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )

        every { neo4jAuthorizationService.userIsAdmin(any()) } returns true

        val entities = searchRepository.getEntities(
            QueryParams(expandedType = "Beekeeper", q = "name==\"Scalpa\""),
            sub,
            offset,
            limit,
            DEFAULT_CONTEXTS
        ).second

        assertTrue(entities.containsAll(listOf(firstEntity.id, secondEntity.id)))
    }

    @Test
    fun `it should return matching entities as the specific access policy`() {
        createEntity(userUri, listOf(AuthorizationService.USER_LABEL), mutableListOf())
        val firstEntity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(
                Property(name = expandedNameProperty, value = "Scalpa"),
                Property(
                    name = JsonLdUtils.EGM_SPECIFIC_ACCESS_POLICY,
                    value = AuthorizationService.SpecificAccessPolicy.AUTH_READ.name
                )
            )
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:1231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )

        val entities = searchRepository.getEntities(
            QueryParams(expandedType = "Beekeeper", q = "name==\"Scalpa\""),
            sub,
            offset,
            limit,
            DEFAULT_CONTEXTS
        ).second

        assertTrue(entities.contains(firstEntity.id))
        assertFalse(entities.contains(secondEntity.id))
    }

    @Test
    fun `it should return matching entities count`() {
        val userEntity = createEntity(userUri, listOf(AuthorizationService.USER_LABEL), mutableListOf())
        val firstEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:01231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:01232".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa2"))
        )
        val thirdEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:03432".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa3"))
        )
        createRelationship(EntitySubjectNode(userEntity.id), R_CAN_WRITE, firstEntity.id)
        createRelationship(EntitySubjectNode(userEntity.id), R_CAN_WRITE, secondEntity.id)
        createRelationship(EntitySubjectNode(userEntity.id), R_CAN_READ, thirdEntity.id)

        val entitiesCount = searchRepository.getEntities(
            QueryParams(expandedType = "Beekeeper", idPattern = "^urn:ngsi-ld:Beekeeper:0.*2$"),
            sub,
            offset,
            limit,
            DEFAULT_CONTEXTS
        ).first

        assertEquals(entitiesCount, 2)
    }

    @Test
    fun `it should return matching entities count when only the count is requested`() {
        val userEntity = createEntity(userUri, listOf(AuthorizationService.USER_LABEL), mutableListOf())
        val firstEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:01231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:01232".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa2"))
        )
        createRelationship(EntitySubjectNode(userEntity.id), R_CAN_WRITE, firstEntity.id)
        createRelationship(EntitySubjectNode(userEntity.id), R_CAN_WRITE, secondEntity.id)

        val countAndEntities = searchRepository.getEntities(
            QueryParams(expandedType = "Beekeeper", idPattern = "^urn:ngsi-ld:Beekeeper:0.*2$"),
            sub,
            offset,
            0,
            DEFAULT_CONTEXTS
        )

        assertEquals(countAndEntities.first, 1)
        assertEquals(countAndEntities.second, emptyList<URI>())
    }

    @ParameterizedTest
    @MethodSource("com.egm.stellio.entity.util.QueryEntitiesParameterizedTests#rawResultsProvider")
    fun `it should only return matching entities requested by pagination`(
        idPattern: String?,
        offset: Int,
        limit: Int,
        expectedEntitiesIds: List<URI>
    ) {
        val userEntity = createEntity(userUri, listOf(AuthorizationService.USER_LABEL), mutableListOf())
        val firstEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:01231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:01232".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa2"))
        )
        val thirdEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:03432".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa3"))
        )
        createRelationship(EntitySubjectNode(userEntity.id), R_CAN_READ, firstEntity.id)
        createRelationship(EntitySubjectNode(userEntity.id), R_CAN_READ, secondEntity.id)
        createRelationship(EntitySubjectNode(userEntity.id), R_CAN_READ, thirdEntity.id)

        val entities = searchRepository.getEntities(
            QueryParams(expandedType = "Beekeeper", idPattern = idPattern),
            sub,
            offset,
            limit,
            DEFAULT_CONTEXTS
        ).second

        assertTrue(entities.containsAll(expectedEntitiesIds))
    }

    fun createEntity(
        id: URI,
        type: List<String>,
        properties: MutableList<Property> = mutableListOf(),
        location: String? = null
    ): Entity {
        val entity = Entity(id = id, type = type, properties = properties, location = location)
        return entityRepository.save(entity)
    }

    fun createRelationship(
        subjectNodeInfo: SubjectNodeInfo,
        relationshipType: String,
        objectId: URI,
        datasetId: URI? = null
    ): Relationship {
        val relationship = Relationship(objectId = objectId, type = listOf(relationshipType), datasetId = datasetId)
        neo4jRepository.createRelationshipOfSubject(subjectNodeInfo, relationship, objectId)

        return relationship
    }
}
