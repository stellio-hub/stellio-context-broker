package com.egm.stellio.search.service

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.net.URI

@Component
class EntityAttributeCleanerService(
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val attributeInstanceService: AttributeInstanceService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    private val coroutineScope = CoroutineScope(Dispatchers.Default)

    fun deleteEntityAttributes(entityId: URI): Job =
        coroutineScope.launch {
            attributeInstanceService.deleteInstancesOfEntity(entityId)
                .onLeft {
                    logger.error("Unable to delete attributes instances of $entityId: $it")
                }
            temporalEntityAttributeService.deleteTemporalAttributesOfEntity(entityId)
                .onLeft {
                    logger.error("Unable to delete attributes of $entityId: $it")
                }
        }
}
