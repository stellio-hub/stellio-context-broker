package com.egm.stellio.entity.config

import org.springframework.cache.CacheManager
import org.springframework.cache.annotation.EnableCaching
import org.springframework.cache.concurrent.ConcurrentMapCacheManager
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

const val SUBJECT_URI_CACHE = "subject-uri"
const val SUBJECT_ROLES_CACHE = "subject-roles"
const val SUBJECT_GROUPS_CACHE = "subject-groups"

@Configuration
@EnableCaching
class CacheConfig {

    @Bean
    fun cacheManager(): CacheManager =
        ConcurrentMapCacheManager(SUBJECT_URI_CACHE, SUBJECT_ROLES_CACHE, SUBJECT_GROUPS_CACHE)
}
