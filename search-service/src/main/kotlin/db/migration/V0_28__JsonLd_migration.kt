package db.migration

import arrow.core.Either
import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.util.toTemporalAttributeMetadata
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.NgsiLdAttributeInstance
import com.egm.stellio.shared.model.toNgsiLdEntity
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SAP
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
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
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.SingleConnectionDataSource
import java.net.URI
import java.time.ZonedDateTime
import java.util.UUID

@Suppress("unused")
class V0_28__JsonLd_migration : BaseJavaMigration() {

    // not so nice since it is specific to deployments
    // but no other easy way to migrate terms that are actually stored compacted in entities payloads
    private val keysToTransform = mapOf(
        "https://uri.etsi.org/ngsi-ld/default-context/dcDescription" to "http://purl.org/dc/terms/description",
        "https://uri.etsi.org/ngsi-ld/default-context/dcTitle" to "http://purl.org/dc/terms/title"
    )

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
            logger.debug("Migrating entity $entityId")
            val deserializedPayload = payload.deserializeAsMap()
            val contexts = extractContextFromInput(deserializedPayload)
            val originalExpandedEntity = expandDeserializedPayload(deserializedPayload, contexts)
                .mapKeys {
                    // replace the faulty expanded terms (only at the rool level of the entity)
                    if (keysToTransform.containsKey(it.key))
                        keysToTransform[it.key]!!
                    else it.key
                }

            // extract specific access policy (if any) from the payload to be able to store it in entity_payload
            // then remove it from the expanded payload
            val specificAccessPolicy =
                getAttributeFromExpandedAttributes(originalExpandedEntity, AUTH_PROP_SAP, null)?.let {
                    getPropertyValueFromMapAsString(it as Map<String, List<Any>>, NGSILD_PROPERTY_VALUE)
                }?.let {
                    AuthContextModel.SpecificAccessPolicy.valueOf(it)
                }
            val expandedEntity = originalExpandedEntity
                .filterKeys { attributeName ->
                    // remove specific access policy attribute as it is not a "normal" attribute
                    attributeName != AUTH_PROP_SAP
                }

            // store the expanded entity payload instead of the compacted one
            val serializedJsonLdEntity = serializeObject(expandedEntity).replace("'", "''")
            jdbcTemplate.execute(
                """
                update entity_payload
                set payload = '$serializedJsonLdEntity',
                    specific_access_policy = ${specificAccessPolicy.toSQLValue()}
                where entity_id = '$entityId'
                """.trimIndent()
            )

            val jsonLdEntity = JsonLdEntity(expandedEntity, contexts)
            val ngsiLdEntity = jsonLdEntity.toNgsiLdEntity()

            // in current implementation, geoproperties do not have a creation date as they are stored
            // as a property of the entity node, so we give them the creation date of the entity
            val defaultCreatedAt = getPropertyValueFromMapAsDateTime(
                expandedEntity as Map<String, List<Any>>,
                NGSILD_CREATED_AT_PROPERTY
            ) ?: ZonedDateTime.parse("1970-01-01T00:00:00Z")

            ngsiLdEntity.attributes.forEach { ngsiLdAttribute ->
                val attributeName = ngsiLdAttribute.name
                ngsiLdAttribute.getAttributeInstances().forEach { ngsiLdAttributeInstance ->
                    val datasetId = ngsiLdAttributeInstance.datasetId
                    val attributePayload = getAttributeFromExpandedAttributes(
                        jsonLdEntity.properties,
                        attributeName,
                        datasetId
                    )!! as Map<String, List<Any>>

                    val attributePayloadFiltered = attributePayload
                        .filterKeys { attributeName ->
                            // remove createdAt and modifiedAt in attribute's payload
                            attributeName != NGSILD_CREATED_AT_PROPERTY && attributeName != NGSILD_MODIFIED_AT_PROPERTY
                        }

                    if (entityHasAttribute(entityId, attributeName, datasetId)) {
                        logger.debug("Attribute $attributeName ($datasetId) exists, adding metadata and payload")
                        updateTeaPayloadAndDates(
                            entityId,
                            attributeName,
                            datasetId,
                            attributePayloadFiltered,
                            ngsiLdAttributeInstance
                        )
                    } else {
                        // create attributes that do not exist
                        //   - non observed attributes created before we kept track of their history
                        //   - attributes of type GeoProperty
                        logger.debug("Attribute $attributeName ($datasetId) does not exist, bootstrapping entry")
                        createTeaEntry(
                            entityId,
                            attributeName,
                            datasetId,
                            attributePayloadFiltered,
                            ngsiLdAttributeInstance,
                            defaultCreatedAt
                        )
                    }
                }
            }
        }
    }

    private fun createTeaEntry(
        entityId: URI,
        attributeName: String,
        datasetId: URI?,
        attributePayload: Map<String, List<Any>>,
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
                        ('$teaId', '$entityId', '$attributeName', '$atributeType', '$attributeValueType', 
                            '$createdAt', ${modifiedAt.toSQLValue()}, ${datasetId.toSQLValue()},
                            '$serializedAttributePayload')
                    """.trimIndent()
                )
                val attributeInstanceId = "urn:ngsi-ld:Instance:${UUID.randomUUID()}".toUri()
                val attributeInstanceQuery = if (temporalAttributesMetadata.value.geoValue != null)
                    """
                    INSERT INTO attribute_instance_audit
                        (time, time_property, measured_value, value, geo_value, 
                            temporal_entity_attribute, instance_id, payload)
                    VALUES
                        ('$createdAt', '${AttributeInstance.TemporalProperty.CREATED_AT}', 
                            ${temporalAttributesMetadata.value.measuredValue}, 
                            ${temporalAttributesMetadata.value.value.toSQLValue()}, 
                            ST_GeomFromText('${temporalAttributesMetadata.value.geoValue!!.value}'), 
                            '$teaId', '$attributeInstanceId', '$serializedAttributePayload')
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
                            '$teaId', '$attributeInstanceId', '$serializedAttributePayload')
                    """.trimIndent()
                jdbcTemplate.execute(attributeInstanceQuery)
            }
        }
    }

    private fun updateTeaPayloadAndDates(
        entityId: URI,
        attributeName: String,
        datasetId: URI?,
        attributePayload: Map<String, List<Any>>,
        ngsiLdAttributeInstance: NgsiLdAttributeInstance
    ) {
        val createdAt = ngsiLdAttributeInstance.createdAt
        val modifiedAt = ngsiLdAttributeInstance.modifiedAt
        val serializedAttributePayload = serializeObject(attributePayload).replace("'", "''")
        jdbcTemplate.execute(
            """
            update temporal_entity_attribute
            set payload = '$serializedAttributePayload',
                created_at = '$createdAt',
                modified_at = ${modifiedAt.toSQLValue()}
            where entity_id = '$entityId'
            and attribute_name = '$attributeName'
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
            where entity_id = '$entityId'
            and attribute_name = '$attributeName'
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
            "and dataset_id = '$this'"

    private fun Any?.toSQLValue(): String? =
        if (this == null) null
        else "'$this'"
}
