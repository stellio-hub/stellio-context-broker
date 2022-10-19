package db.migration

import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.expandDeserializedPayload
import com.egm.stellio.shared.util.JsonLdUtils.extractContextFromInput
import com.egm.stellio.shared.util.JsonLdUtils.getAttributeFromExpandedAttributes
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMapAsDateTime
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.toUri
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.SingleConnectionDataSource
import java.util.UUID

class V0_28__JsonLd_migration : BaseJavaMigration() {

    private val logger = LoggerFactory.getLogger(javaClass)

    override fun migrate(context: Context) {
        val jdbcTemplate = JdbcTemplate(SingleConnectionDataSource(context.connection, true))
        jdbcTemplate.queryForStream(
            """
            select entity_id, payload
            from entity_payload
            """.trimMargin()
        ) { resultSet, _ ->
            Pair(resultSet.getString("entity_id"), resultSet.getString("payload"))
        }.forEach { (entityId, payload) ->
            logger.debug("Migrating entity $entityId")
            // store the expanded entity payload instead of the compacted one
            val deserializedPayload = payload.deserializeAsMap()
            val contexts = extractContextFromInput(deserializedPayload)
            val expandedEntity = expandDeserializedPayload(deserializedPayload, contexts)
            logger.debug("Expanded entity is $expandedEntity")
            val jsonLdEntity = JsonLdEntity(expandedEntity, contexts)
            logger.debug(expandedEntity.toString())
            val serializedJsonLdEntity = "$$${serializeObject(expandedEntity)}$$"
            jdbcTemplate.execute(
                """
                update entity_payload
                set payload = $serializedJsonLdEntity
                where entity_id = '$entityId'
                """.trimIndent()
            )

            updateTeaPayloadAndDates(jdbcTemplate, entityId, jsonLdEntity)

            // TODO create attributes that do not exist yet
        }
    }

    private fun updateTeaPayloadAndDates(
        jdbcTemplate: JdbcTemplate,
        entityId: String,
        jsonLdEntity: JsonLdEntity
    ) {
        jdbcTemplate.queryForStream(
            """
            select id, attribute_name, dataset_id
            from temporal_entity_attribute
            where entity_id = '$entityId'
            """.trimIndent()
        ) { teaResultSet, _ ->
            val teaUuid = UUID.fromString(teaResultSet.getString("id"))
            val attributeName = teaResultSet.getString("attribute_name")
            val datasetId = teaResultSet.getString("dataset_id")?.toUri()
            logger.debug("Searching attribute $attributeName ($datasetId) from $teaUuid in entity $entityId")
            val attributePayload = getAttributeFromExpandedAttributes(
                jsonLdEntity.properties,
                attributeName,
                datasetId
            )
            Pair(teaUuid, attributePayload)
        }.forEach { (teaUuid, teaExpandedPayload) ->
            if (teaExpandedPayload != null) {
                val createdAt =
                    getPropertyValueFromMapAsDateTime(teaExpandedPayload as Map<String, List<Any>>, NGSILD_CREATED_AT_PROPERTY)
                val modifiedAt =
                    getPropertyValueFromMapAsDateTime(teaExpandedPayload, NGSILD_MODIFIED_AT_PROPERTY)
                val serializedAttributePayload = "$$${serializeObject(teaExpandedPayload)}$$"
                if (modifiedAt != null)
                    jdbcTemplate.execute(
                        """
                        update temporal_entity_attribute
                        set payload = $serializedAttributePayload,
                            created_at = '$createdAt',
                            modified_at = '$modifiedAt'
                        where id = '$teaUuid'
                        """.trimIndent()
                    )
                else
                    jdbcTemplate.execute(
                        """
                        update temporal_entity_attribute
                        set payload = $serializedAttributePayload,
                            created_at = '$createdAt'
                        where id = '$teaUuid'
                        """.trimIndent()
                    )
            } else {
                logger.warn("Did not find payload for attribute $teaUuid of entity $entityId")
            }
        }
    }
}
