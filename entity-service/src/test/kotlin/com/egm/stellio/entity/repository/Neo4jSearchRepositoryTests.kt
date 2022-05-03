package com.egm.stellio.entity.repository

import arrow.core.Some
import com.egm.stellio.entity.authorization.Neo4jAuthorizationService
import com.egm.stellio.entity.config.WithNeo4jContainer
import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_ROLES
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SAP
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SID
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_ADMIN
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_READ
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_WRITE
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_IS_MEMBER_OF
import com.egm.stellio.shared.util.AuthContextModel.CLIENT_TYPE
import com.egm.stellio.shared.util.AuthContextModel.GROUP_TYPE
import com.egm.stellio.shared.util.AuthContextModel.USER_TYPE
import com.egm.stellio.shared.util.DEFAULT_CONTEXTS
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import java.net.URI
import java.util.*

@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = ["application.authentication.enabled=true"])
class Neo4jSearchRepositoryTests : WithNeo4jContainer {

    @Autowired
    private lateinit var searchRepository: SearchRepository

    @Autowired
    private lateinit var neo4jRepository: Neo4jRepository

    @Autowired
    private lateinit var entityRepository: EntityRepository

    @MockkBean(relaxed = true)
    private lateinit var neo4jAuthorizationService: Neo4jAuthorizationService

    private val sub = Some(UUID.randomUUID().toString())
    private val beekeeperUri = "urn:ngsi-ld:Beekeeper:1230".toUri()
    private val groupUri = "urn:ngsi-ld:Group:01".toUri()
    private val userUri = (AuthContextModel.USER_PREFIX + sub.value).toUri()
    private val clientUri = "urn:ngsi-ld:Client:01".toUri()
    private val serviceAccountUri = userUri
    private val expandedNameProperty = expandJsonLdTerm("name", DEFAULT_CONTEXTS)!!
    private val offset = 0
    private val limit = 20

    @BeforeEach
    fun createGlobalMockResponses() {
        every { neo4jAuthorizationService.getSubjectUri(sub) } returns userUri
    }

    @AfterEach
    fun cleanData() {
        entityRepository.deleteAll()
    }

    @Test
    fun `it should return matching entities that user can access`() {
        val userEntity = createEntity(userUri, listOf(USER_TYPE), mutableListOf())
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
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_WRITE, firstEntity.id)
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_ADMIN, secondEntity.id)
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_READ, thirdEntity.id)

        val entities = searchRepository.getEntities(
            QueryParams(types = setOf("Beekeeper"), q = "name==\"Scalpa\"", offset = offset, limit = limit),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertTrue(entities.containsAll(listOf(firstEntity.id, secondEntity.id, thirdEntity.id)))
    }

    @Test
    fun `it should return matching entities that user can access by it's group`() {
        val userEntity = createEntity(userUri, listOf(USER_TYPE), mutableListOf())
        val groupEntity = createEntity(groupUri, listOf(GROUP_TYPE), mutableListOf())
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_IS_MEMBER_OF, groupEntity.id)
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
        createRelationship(EntitySubjectNode(groupEntity.id), AUTH_REL_CAN_WRITE, firstEntity.id)
        createRelationship(EntitySubjectNode(groupEntity.id), AUTH_REL_CAN_WRITE, secondEntity.id)

        every { neo4jAuthorizationService.getSubjectGroups(sub) } returns setOf(groupUri)

        val entities = searchRepository.getEntities(
            QueryParams(types = setOf("Beekeeper"), q = "name==\"Scalpa\"", offset = offset, limit = limit),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertTrue(entities.containsAll(listOf(firstEntity.id, secondEntity.id)))
    }

    @Test
    fun `it should not return a matching entity that user cannot access`() {
        createEntity(userUri, listOf(USER_TYPE), mutableListOf())
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )
        val entities = searchRepository.getEntities(
            QueryParams(types = setOf("Beekeeper"), q = "name==\"Scalpa\"", offset = offset, limit = limit),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertFalse(entities.contains(entity.id))
    }

    @Test
    fun `it should return matching entities that client can access`() {
        val clientEntity = createEntity(
            clientUri,
            listOf(CLIENT_TYPE),
            mutableListOf(
                Property(name = AUTH_PROP_SID, value = serviceAccountUri)
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
        createRelationship(EntitySubjectNode(clientEntity.id), AUTH_REL_CAN_READ, firstEntity.id)
        createRelationship(EntitySubjectNode(clientEntity.id), AUTH_REL_CAN_READ, secondEntity.id)

        every { neo4jAuthorizationService.getSubjectUri(sub) } returns clientUri

        val queryParams =
            QueryParams(types = setOf("Beekeeper"), q = "name==\"Scalpa\"", offset = offset, limit = limit)
        var entities = searchRepository.getEntities(
            queryParams,
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertTrue(entities.containsAll(listOf(firstEntity.id, secondEntity.id)))

        entities = searchRepository.getEntities(
            queryParams.copy(attrs = setOf(expandedNameProperty), offset = offset, limit = limit),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertTrue(entities.containsAll(listOf(firstEntity.id, secondEntity.id)))
    }

    @Test
    fun `it should return all matching entities for admin users`() {
        createEntity(
            userUri,
            listOf(USER_TYPE),
            mutableListOf(
                Property(
                    name = AUTH_PROP_ROLES,
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
            QueryParams(types = setOf("Beekeeper"), q = "name==\"Scalpa\"", offset = offset, limit = limit),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertTrue(entities.containsAll(listOf(firstEntity.id, secondEntity.id)))
    }

    @Test
    fun `it should return matching entities as the specific access policy`() {
        createEntity(userUri, listOf(USER_TYPE), mutableListOf())
        val firstEntity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(
                Property(name = expandedNameProperty, value = "Scalpa"),
                Property(
                    name = AUTH_PROP_SAP,
                    value = AuthContextModel.SpecificAccessPolicy.AUTH_READ.name
                )
            )
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:1231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )

        val entities = searchRepository.getEntities(
            QueryParams(types = setOf("Beekeeper"), q = "name==\"Scalpa\"", offset = offset, limit = limit),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertTrue(entities.contains(firstEntity.id))
        assertFalse(entities.contains(secondEntity.id))
    }

    @Test
    fun `it should return matching entities count`() {
        val userEntity = createEntity(userUri, listOf(USER_TYPE), mutableListOf())
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
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_WRITE, firstEntity.id)
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_WRITE, secondEntity.id)
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_READ, thirdEntity.id)

        val entitiesCount = searchRepository.getEntities(
            QueryParams(
                types = setOf("Beekeeper"),
                idPattern = "^urn:ngsi-ld:Beekeeper:0.*2$",
                offset = offset,
                limit = limit
            ),
            sub,
            DEFAULT_CONTEXTS
        ).first

        assertEquals(2, entitiesCount)
    }

    @Test
    fun `it should return matching entities count when only the count is requested`() {
        val userEntity = createEntity(userUri, listOf(USER_TYPE), mutableListOf())
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
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_WRITE, firstEntity.id)
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_WRITE, secondEntity.id)

        val countAndEntities = searchRepository.getEntities(
            QueryParams(
                types = setOf("Beekeeper"),
                idPattern = "^urn:ngsi-ld:Beekeeper:0.*2$",
                offset = offset,
                limit = 0
            ),
            sub,
            DEFAULT_CONTEXTS
        )

        assertEquals(1, countAndEntities.first)
        assertEquals(emptyList<URI>(), countAndEntities.second)
    }

    @ParameterizedTest
    @MethodSource("com.egm.stellio.entity.util.QueryEntitiesParameterizedTests#rawResultsProvider")
    fun `it should only return matching entities requested by pagination`(
        idPattern: String?,
        offset: Int,
        limit: Int,
        expectedEntitiesIds: List<URI>
    ) {
        val userEntity = createEntity(userUri, listOf(USER_TYPE), mutableListOf())
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
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_READ, firstEntity.id)
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_READ, secondEntity.id)
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_READ, thirdEntity.id)

        val entities = searchRepository.getEntities(
            QueryParams(types = setOf("Beekeeper"), idPattern = idPattern, offset = offset, limit = limit),
            sub,
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
        val entity = Entity(id = id, types = type, properties = properties, location = location)
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
