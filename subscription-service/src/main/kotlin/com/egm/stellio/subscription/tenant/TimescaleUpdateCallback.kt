package com.egm.stellio.subscription.tenant

import org.flywaydb.core.api.callback.Callback
import org.flywaydb.core.api.callback.Context
import org.flywaydb.core.api.callback.Event
import org.springframework.jdbc.datasource.SimpleDriverDataSource
import java.sql.DriverManager
import java.util.Properties

class TimescaleUpdateCallback : Callback {

    override fun supports(event: Event, context: Context?): Boolean = event == Event.BEFORE_MIGRATE

    override fun canHandleInTransaction(event: Event, context: Context?): Boolean = false

    override fun handle(event: Event, context: Context) {
        val ds = context.configuration.dataSource as SimpleDriverDataSource
        val props = Properties().apply {
            setProperty("user", ds.username ?: "")
            setProperty("password", ds.password ?: "")
            // prevents TimescaleDB from initializing its hooks in this session,
            // which is required for ALTER EXTENSION to succeed
            setProperty("options", "-c timescaledb.disable_load=on")
        }
        DriverManager.getConnection(ds.url, props).use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("ALTER EXTENSION timescaledb UPDATE")
            }
        }
    }

    override fun getCallbackName(): String = "TimescaleUpdateCallback"
}
