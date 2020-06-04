package com.egm.stellio.entity.config

import org.neo4j.ogm.session.SessionFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.neo4j.repository.config.EnableNeo4jRepositories
import org.springframework.data.neo4j.transaction.Neo4jTransactionManager
import org.springframework.transaction.annotation.EnableTransactionManagement

@Configuration
@EnableNeo4jRepositories(basePackages = ["com.egm.stellio.entity.repository"])
@EnableTransactionManagement
class Neo4jConfiguration(
    private val sessionFactory: SessionFactory
) {

    @Bean
    fun transactionManager(): Neo4jTransactionManager {
        return Neo4jTransactionManager(sessionFactory)
    }
}
