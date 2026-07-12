package com.egm.stellio.search.entity.job

import com.egm.stellio.search.entity.service.EntityService
import com.egm.stellio.search.entity.web.BatchOperationResult
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.config.ApplicationProperties.TenantConfiguration
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.confirmVerified
import io.mockk.every
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    classes = [TransientDataGarbageCollectionJob::class]
)
@ActiveProfiles("test")
class TransientDataGarbageCollectionJobTests {

    @Autowired
    private lateinit var transientDataGarbageCollectionJob: TransientDataGarbageCollectionJob

    @MockkBean
    private lateinit var entityService: EntityService

    @MockkBean
    private lateinit var applicationProperties: ApplicationProperties

    @Test
    fun `purgeTransientData should call purgeExpiredEntitiesAndAttributes once per configured tenant`() {
        every { applicationProperties.tenants } returns listOf(
            TenantConfiguration("urn:ngsi-ld:tenant:A", ""),
            TenantConfiguration("urn:ngsi-ld:tenant:B", "")
        )
        coEvery { entityService.purgeExpiredEntitiesAndAttributes() } returns BatchOperationResult()

        transientDataGarbageCollectionJob.purgeTransientData()

        coVerify(exactly = 2, timeout = 1000L) { entityService.purgeExpiredEntitiesAndAttributes() }
        confirmVerified(entityService)
    }
}
