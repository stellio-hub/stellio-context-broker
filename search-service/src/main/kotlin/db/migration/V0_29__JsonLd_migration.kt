package db.migration

import arrow.core.Either
import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.util.guessPropertyValueType
import com.egm.stellio.search.util.toTemporalAttributeMetadata
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SAP
import com.egm.stellio.shared.util.ExpandedAttributeInstance
import com.egm.stellio.shared.util.ExpandedTerm
import com.egm.stellio.shared.util.JsonLdUtils.EGM_BASE_CONTEXT_URL
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_EXPANDED_ENTITY_CORE_MEMBERS
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.expandDeserializedPayload
import com.egm.stellio.shared.util.JsonLdUtils.extractContextFromInput
import com.egm.stellio.shared.util.JsonLdUtils.getAttributeFromExpandedAttributes
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMapAsDateTime
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMapAsString
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.toUri
import kotlinx.coroutines.runBlocking
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.SingleConnectionDataSource
import java.net.URI
import java.time.ZonedDateTime
import java.time.format.DateTimeParseException
import java.util.UUID

const val EGM_NO_BRANCH_BASE_CONTEXT_URL =
    "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models"

@Suppress("unused")
class V0_29__JsonLd_migration : BaseJavaMigration() {

    // not so nice since it is specific to deployments
    // but no other easy way to migrate terms that are actually stored compacted in entities payloads
    private val keysToTransform = mapOf(
        "https://uri.etsi.org/ngsi-ld/default-context/dcDescription" to "http://purl.org/dc/terms/description",
        "https://uri.etsi.org/ngsi-ld/default-context/dcTitle" to "http://purl.org/dc/terms/title"
    )

    private val contextsToTransform = mapOf(
        "https://schema.lab.fiware.org/ld/context.jsonld" to
            "$EGM_BASE_CONTEXT_URL/fiware/jsonld-contexts/labFiware-compound.jsonld",
        "$EGM_NO_BRANCH_BASE_CONTEXT_URL/feature/mlaas-models/mlaas/jsonld-contexts/mlaas-compound.jsonld" to
            "$EGM_BASE_CONTEXT_URL/mlaas/jsonld-contexts/mlaas-ngsild-compound.jsonld",
        "$EGM_NO_BRANCH_BASE_CONTEXT_URL/ngsi-ld-v1.2/mapping/jsonld-contexts/mapping-compound.jsonld" to
            "$EGM_BASE_CONTEXT_URL/mapping/jsonld-contexts/mapping-compound.jsonld",
        "$EGM_BASE_CONTEXT_URL/agriMushroom/jsonld-contexts/agri-mushroom-compound.jsonld" to
            "$EGM_BASE_CONTEXT_URL/graced/jsonld-contexts/graced.jsonld"
    )

    private val defaultZonedDateTime = ZonedDateTime.parse("1970-01-01T00:00:00Z")

    private val logger = LoggerFactory.getLogger(javaClass)
    private lateinit var jdbcTemplate: JdbcTemplate

    override fun migrate(context: Context) {
        jdbcTemplate = JdbcTemplate(SingleConnectionDataSource(context.connection, true))
        // use the stored entity payloads to populate and fix the stored data
        jdbcTemplate.queryForStream(
            """
            select entity_id, payload
            from entity_payload
            """.trimMargin()
        ) { resultSet, _ ->
            Pair(resultSet.getString("entity_id").toUri(), resultSet.getString("payload"))
        }.forEach { (entityId, payload) ->
            logger.debug("Migrating entity {}", entityId)
            val deserializedPayload = payload.deserializeAsMap()
            val contexts = extractContextFromInput(deserializedPayload)
                .map {
                    if (contextsToTransform.containsKey(it)) {
                        contextsToTransform[it]!!
                    } else it
                }
            val originalExpandedEntity = runBlocking {
                expandDeserializedPayload(deserializedPayload, contexts)
                    .mapKeys {
                        // replace the faulty expanded terms (only at the rool level of the entity)
                        if (keysToTransform.containsKey(it.key))
                            keysToTransform[it.key]!!
                        else it.key
                    }
                    .keepOnlyOneInstanceByDatasetId()
            }
            // extract specific access policy (if any) from the payload to be able to store it in entity_payload
            // then remove it from the expanded payload
            val specificAccessPolicy =
                getAttributeFromExpandedAttributes(originalExpandedEntity, AUTH_PROP_SAP, null)?.let {
                    getPropertyValueFromMapAsString(it, NGSILD_PROPERTY_VALUE)
                }?.let {
                    AuthContextModel.SpecificAccessPolicy.valueOf(it)
                }
            val expandedEntity = originalExpandedEntity
                .filterKeys { attributeName ->
                    // remove specific access policy attribute as it is not a "normal" attribute
                    attributeName != AUTH_PROP_SAP
                }

            // in current implementation, geoproperties do not have a creation date as they are stored
            // as a property of the entity node, so we give them the creation date of the entity
            val entityCreationDate =
                try {
                    getPropertyValueFromMapAsDateTime(
                        expandedEntity as Map<String, List<Any>>,
                        NGSILD_CREATED_AT_PROPERTY
                    ) ?: defaultZonedDateTime
                } catch (e: DateTimeParseException) {
                    logger.warn(
                        "Unable to parse creation date (${e.message}) for entity $entityId, using default date"
                    )
                    defaultZonedDateTime
                }

            // store the expanded entity payload instead of the compacted one
            val serializedJsonLdEntity = serializeObject(expandedEntity)
            jdbcTemplate.execute(
                """
                update entity_payload
                set created_at = '$entityCreationDate',
                    payload = $$$serializedJsonLdEntity$$,
                    specific_access_policy = ${specificAccessPolicy.toSQLValue()},
                    contexts = ${contexts.toSqlArray()}
                where entity_id = $$$entityId$$
                """.trimIndent()
            )

            val jsonLdEntity = JsonLdEntity(expandedEntity, contexts)
            val ngsiLdEntity = runBlocking {
                jsonLdEntity.toNgsiLdEntity()
            }

            when (ngsiLdEntity) {
                is Either.Left -> logger.warn("Unable to transform input to NGSI-LD entity: $jsonLdEntity")
                is Either.Right ->
                    ngsiLdEntity.value.attributes.forEach { ngsiLdAttribute ->
                        val attributeName = ngsiLdAttribute.name
                        ngsiLdAttribute.getAttributeInstances().forEach { ngsiLdAttributeInstance ->
                            val datasetId = ngsiLdAttributeInstance.datasetId
                            val attributePayload = getAttributeFromExpandedAttributes(
                                jsonLdEntity.members,
                                attributeName,
                                datasetId
                            )!!

                            val attributePayloadFiltered = attributePayload
                                .filterKeys { attributeName ->
                                    // remove createdAt and modifiedAt from attribute payload
                                    attributeName != NGSILD_CREATED_AT_PROPERTY &&
                                        attributeName != NGSILD_MODIFIED_AT_PROPERTY
                                }

                            if (entityHasAttribute(entityId, attributeName, datasetId)) {
                                logger.debug(
                                    "Attribute {} ({}) exists, adding metadata and payload",
                                    attributeName,
                                    datasetId
                                )
                                updateTeaPayloadAndDates(
                                    entityId,
                                    attributeName,
                                    datasetId,
                                    attributePayloadFiltered,
                                    ngsiLdAttributeInstance,
                                    entityCreationDate
                                )
                            } else {
                                // create attributes that do not exist
                                //   - non observed attributes created before we kept track of their history
                                //   - attributes of type GeoProperty
                                logger.debug(
                                    "Attribute {} ({}) does not exist, bootstrapping entry",
                                    attributeName,
                                    datasetId
                                )
                                createTeaEntry(
                                    entityId,
                                    attributeName,
                                    datasetId,
                                    attributePayloadFiltered,
                                    ngsiLdAttributeInstance,
                                    entityCreationDate
                                )
                            }
                        }
                    }
            }
        }
    }

    private fun createTeaEntry(
        entityId: URI,
        attributeName: ExpandedTerm,
        datasetId: URI?,
        attributePayload: ExpandedAttributeInstance,
        ngsiLdAttributeInstance: NgsiLdAttributeInstance,
        defaultCreatedAt: ZonedDateTime
    ) {
        when (val temporalAttributesMetadata = ngsiLdAttributeInstance.toTemporalAttributeMetadata()) {
            is Either.Left ->
                logger.warn("Unable to process attribute $attributeName ($datasetId) from entity $entityId")
            is Either.Right -> {
                val createdAt = ngsiLdAttributeInstance.createdAt ?: defaultCreatedAt
                val modifiedAt = ngsiLdAttributeInstance.modifiedAt
                val atributeType = temporalAttributesMetadata.value.type
                val attributeValueType = temporalAttributesMetadata.value.valueType
                val serializedAttributePayload = serializeObject(attributePayload)
                val teaId = UUID.randomUUID()
                jdbcTemplate.execute(
                    """
                    INSERT INTO temporal_entity_attribute
                        (id, entity_id, attribute_name, attribute_type, attribute_value_type, 
                            created_at, modified_at, dataset_id, payload)
                    VALUES 
                        ('$teaId', $$$entityId$$, $$$attributeName$$, '$atributeType', '$attributeValueType', 
                            '$createdAt', ${modifiedAt.toSQLValue()}, ${datasetId.toSQLValue()},
                            $$$serializedAttributePayload$$)
                    """.trimIndent()
                )
                val attributeInstanceId = "urn:ngsi-ld:Instance:${UUID.randomUUID()}".toUri()
                val attributeInstanceQuery = if (temporalAttributesMetadata.value.geoValue != null)
                    """
                    INSERT INTO attribute_instance_audit
                        (time, time_property, geo_value, 
                            temporal_entity_attribute, instance_id, payload)
                    VALUES
                        ('$createdAt', '${AttributeInstance.TemporalProperty.CREATED_AT}', 
                            public.ST_GeomFromText('${temporalAttributesMetadata.value.geoValue!!.value}'), 
                            '$teaId', '$attributeInstanceId', $$$serializedAttributePayload$$)
                    """.trimIndent()
                else
                    """
                    INSERT INTO attribute_instance_audit
                        (time, time_property, measured_value, value, 
                            temporal_entity_attribute, instance_id, payload)
                    VALUES
                        ('$createdAt', '${AttributeInstance.TemporalProperty.CREATED_AT}', 
                            ${temporalAttributesMetadata.value.measuredValue}, 
                            ${temporalAttributesMetadata.value.value.toSQLValue()}, 
                            '$teaId', '$attributeInstanceId', $$$serializedAttributePayload$$)
                    """.trimIndent()
                jdbcTemplate.execute(attributeInstanceQuery)
            }
        }
    }

    private fun updateTeaPayloadAndDates(
        entityId: URI,
        attributeName: ExpandedTerm,
        datasetId: URI?,
        attributePayload: ExpandedAttributeInstance,
        ngsiLdAttributeInstance: NgsiLdAttributeInstance,
        defaultCreatedAt: ZonedDateTime
    ) {
        val createdAt = ngsiLdAttributeInstance.createdAt ?: defaultCreatedAt
        val modifiedAt = ngsiLdAttributeInstance.modifiedAt
        val serializedAttributePayload = serializeObject(attributePayload)

        val valueType = when (ngsiLdAttributeInstance) {
            is NgsiLdPropertyInstance -> guessPropertyValueType(ngsiLdAttributeInstance).first
            is NgsiLdRelationshipInstance -> TemporalEntityAttribute.AttributeValueType.URI
            is NgsiLdGeoPropertyInstance -> TemporalEntityAttribute.AttributeValueType.GEOMETRY
        }

        jdbcTemplate.execute(
            """
            update temporal_entity_attribute
            set payload = $$$serializedAttributePayload$$,
                created_at = '$createdAt',
                modified_at = ${modifiedAt.toSQLValue()},
                attribute_value_type = '$valueType'
            where entity_id = $$$entityId$$
            and attribute_name = $$$attributeName$$
            ${datasetId.toSQLFilter()}
            """.trimIndent()
        )
    }

    private fun entityHasAttribute(
        entityId: URI,
        attributeName: ExpandedTerm,
        datasetId: URI?
    ): Boolean {
        return jdbcTemplate.queryForStream(
            """
            select count(*) as count
            from temporal_entity_attribute
            where entity_id = $$$entityId$$
            and attribute_name = $$$attributeName$$
            ${datasetId.toSQLFilter()}
            """.trimIndent()
        ) { resultSet, _ ->
            resultSet.getLong("count") > 0
        }.toList().first()
    }

    private fun URI?.toSQLFilter(): String =
        if (this == null)
            "and dataset_id is null"
        else
            "and dataset_id = $$$this$$"

    private fun Any?.toSQLValue(): String? =
        if (this == null) null
        else "$$$this$$"

    private fun List<String>.toSqlArray(): String =
        "ARRAY[${this.joinToString(separator = "','", prefix = "'", postfix = "'")}]"
}

internal fun Map<String, Any>.keepOnlyOneInstanceByDatasetId(): Map<String, Any> =
    this.mapValues {
        val instance =
            if (!JSONLD_EXPANDED_ENTITY_CORE_MEMBERS.contains(it.key) && it.value is List<*>) {
                (it.value as List<Map<String, Any>>).distinctBy {
                    it[NGSILD_DATASET_ID_PROPERTY]
                }
            } else it.value
        instance
    }
