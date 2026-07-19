package com.egm.stellio.search.entity.job

import com.egm.stellio.search.entity.service.EntityService
import com.egm.stellio.search.entity.web.BatchOperationResult
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.util.NGSILD_TENANT_HEADER
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.reactor.mono
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

/**
 * NGSI-LD 4.22 - Transient Storage of Entities and Attributes: periodically garbage-collects, for each configured
 * tenant, entities and attributes whose expiresAt lies in the past. Per the spec, clean-up processes only run
 * periodically and final deletion always lags the expiresAt timestamp.
 */
@Component
class TransientDataGarbageCollectionJob(
    private val entityService: EntityService,
    private val applicationProperties: ApplicationProperties
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @Scheduled(cron = $$"${search.transient-data.gc.interval:0 0 4 * * *}")
    fun purgeTransientData() {
        applicationProperties.tenants.forEach { tenantConfiguration ->
            runBlocking {
                mono {
                    entityService.purgeExpiredEntitiesAndAttributes()
                }.contextWrite {
                    it.put(NGSILD_TENANT_HEADER, tenantConfiguration.name)
                }.doOnError {
                    logger.error("Purge of transient data failed for tenant {}", tenantConfiguration.name, it)
                }.onErrorComplete()
                    .awaitSingleOrNull()
                    ?.let { result -> logPurgeResult(result, tenantConfiguration.name) }
            }
        }
    }

    private fun logPurgeResult(result: BatchOperationResult, tenantName: String) {
        if (result.success.isNotEmpty() || result.errors.isNotEmpty())
            logger.debug(
                "Purged {} expired entities/attributes in tenant {} ({} errors)",
                result.success.size,
                tenantName,
                result.errors.size
            )
        result.errors.forEach {
            logger.warn(
                "Error while purging expired entity {} in tenant {}: {}",
                it.entityId,
                tenantName,
                it.error
            )
        }
    }
}
