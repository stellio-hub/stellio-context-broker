// ktlint-disable filename

package db.migration

import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.toUri
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.SingleConnectionDataSource
import java.net.URI

const val LIMIT = 1000
const val OFFSET = 0

@Suppress("unused")
class V0_33__Expand_attribute_instance_payload : BaseJavaMigration() {

    private val logger = LoggerFactory.getLogger(javaClass)
    private lateinit var jdbcTemplate: JdbcTemplate

    override fun migrate(context: Context) {
        jdbcTemplate = JdbcTemplate(SingleConnectionDataSource(context.connection, true))

        queryAttributeUpdateByPage("attribute_instance", LIMIT, OFFSET)
        queryAttributeUpdateByPage("attribute_instance_audit", LIMIT, OFFSET)
    }

    private fun queryAttributeUpdateByPage(tableName: String, limit: Int, offset: Int) {
        jdbcTemplate.queryForStream(
            """
            select instance_id, payload
            from $tableName
            where instance_id = jsonb_path_query_first(payload, '$')->>'instanceId'
            limit $limit
            offset $offset
            """.trimMargin()
        ) { resultSet, _ ->
            Pair(resultSet.getString("instance_id").toUri(), resultSet.getString("payload"))
        }.forEach { (instanceId, payload) ->
            val expandedPayload = expandJsonLdFragment(payload, listOf(NGSILD_CORE_CONTEXT))
            updatePayloadAttributeInstance(
                instanceId!!,
                serializeObject(expandedPayload),
                tableName
            )
        }.let {
            queryAttributeUpdateByPage(tableName, limit, offset + limit)
        }
    }

    private fun updatePayloadAttributeInstance(
        instanceId: URI,
        expandedPayload: String,
        tableName: String
    ) {
        logger.debug("Expand instance $instanceId")
        jdbcTemplate.execute(
            """
            update $tableName
            set payload = $$$expandedPayload$$::jsonb
            where instance_id = $$$instanceId$$
            """.trimIndent()
        )
    }
}
