package com.egm.stellio.entity.config

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.sql.DataSource

@Configuration
class DataSourceConfiguration {

    @Value("\${spring.data.neo4j.uri:}")
    private val jdbcUrl: String = ""
    @Value("\${spring.data.neo4j.username:}")
    private val username: String = ""
    @Value("\${spring.data.neo4j.password:}")
    private val password: String = ""

    @Bean
    fun dataSource(): DataSource {
        val config = HikariConfig()
        config.jdbcUrl = "jdbc:neo4j:$jdbcUrl"
        config.username = username
        config.password = password
        return HikariDataSource(config)
    }
}