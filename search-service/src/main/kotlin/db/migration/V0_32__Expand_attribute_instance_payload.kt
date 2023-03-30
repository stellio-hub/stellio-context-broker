// ktlint-disable filename

package db.migration

import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.toUri
import io.r2dbc.postgresql.codec.Json
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.SingleConnectionDataSource
import java.net.URI

@Suppress("unused")
class V0_32__Expand_attribute_instance_payload : BaseJavaMigration() {

    private val logger = LoggerFactory.getLogger(javaClass)
    private lateinit var jdbcTemplate: JdbcTemplate

    override fun migrate(context: Context) {
        jdbcTemplate = JdbcTemplate(SingleConnectionDataSource(context.connection, true))
        jdbcTemplate.queryForStream(
            """
            select instance_id, payload
            from attribute_instance
            """.trimMargin()
        ) { resultSet, _ ->
            Pair(resultSet.getString("instance_id").toUri(), resultSet.getString("payload"))
        }.forEach { (instanceId, payload) ->
            logger.debug("Expand instance $instanceId")
            val deserializedPayload = payload.deserializeAsMap()
            if (deserializedPayload.containsKey(JSONLD_TYPE)) {
                val expandedPayload = expandJsonLdFragment(deserializedPayload, listOf(NGSILD_CORE_CONTEXT))
                updatePayloadAttributeInstance(
                    instanceId,
                    Json.of(serializeObject(expandedPayload))
                )
            }
        }
        jdbcTemplate.queryForStream(
            """
            select instance_id, payload
            from attribute_instance_audit
            """.trimMargin()
        ) { resultSet, _ ->
            Pair(resultSet.getString("instance_id").toUri(), resultSet.getString("payload"))
        }.forEach { (instanceId, payload) ->
            val deserializedPayload = payload.deserializeAsMap()
            if (deserializedPayload.containsKey(JSONLD_TYPE)) {
                val expandedPayload = expandJsonLdFragment(deserializedPayload, listOf(NGSILD_CORE_CONTEXT))
                updatePayloadAttributeInstanceAudit(
                    instanceId,
                    Json.of(serializeObject(expandedPayload))
                )
            }
        }
    }

    private fun updatePayloadAttributeInstance(
        instanceId: URI,
        expandedPayload: Json
    ) {
        logger.debug("Expand instance $instanceId")
        jdbcTemplate.execute(
            """
            update attribute_instance
            set payload = $$$expandedPayload$$
            where instance_id = $$$instanceId$$
            """.trimIndent()
        )
    }

    private fun updatePayloadAttributeInstanceAudit(
        instanceId: URI,
        expandedPayload: Json
    ) {
        logger.debug("Expand instance $instanceId")
        jdbcTemplate.execute(
            """
            update attribute_instance_audit
            set payload = $$$expandedPayload$$
            where instance_id = $$$instanceId$$
            """.trimIndent()
        )
    }
}
