package com.egm.datahub.context.registry.config

import org.springframework.context.annotation.Configuration
import org.springframework.data.neo4j.repository.config.EnableNeo4jRepositories
import org.springframework.transaction.annotation.EnableTransactionManagement
import org.springframework.data.neo4j.transaction.Neo4jTransactionManager
import org.neo4j.ogm.session.SessionFactory
import org.springframework.context.annotation.Bean

@Configuration
@EnableNeo4jRepositories(basePackages = ["com.egm.datahub.context.registry.repository"])
@EnableTransactionManagement
class Neo4jConfiguration(
    private val sessionFactory: SessionFactory
) {

    @Bean
    fun transactionManager(): Neo4jTransactionManager {
        return Neo4jTransactionManager(sessionFactory)
    }
}
