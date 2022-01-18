package com.egm.stellio.entity

import com.egm.stellio.entity.model.NotUpdatedDetails
import com.egm.stellio.entity.model.UpdateOperationResult
import com.egm.stellio.entity.model.UpdateResult
import com.egm.stellio.entity.model.UpdatedDetails
import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class UpdateResultTest {

    @Test
    fun `it should find the successful update operation results`() {
        assertTrue(UpdateOperationResult.UPDATED.isSuccessResult())
        assertTrue(UpdateOperationResult.APPENDED.isSuccessResult())
        assertTrue(UpdateOperationResult.REPLACED.isSuccessResult())
    }

    @Test
    fun `it should find the failed update operation results`() {
        assertFalse(UpdateOperationResult.FAILED.isSuccessResult())
        assertFalse(UpdateOperationResult.IGNORED.isSuccessResult())
    }

    @Test
    fun `it should find a successful update result`() {
        val updateResult =
            UpdateResult(
                notUpdated = emptyList(),
                updated = listOf(
                    UpdatedDetails("attributeName", "urn:ngsi-ld:Entity:01".toUri(), UpdateOperationResult.UPDATED)
                )
            )

        assertTrue(updateResult.isSuccessful())
    }

    @Test
    fun `it should find a failed update result if there is one not updated attribute`() {
        val updateResult =
            UpdateResult(
                notUpdated = listOf(
                    NotUpdatedDetails("attributeName", "attribute is malformed")
                ),
                updated = listOf(
                    UpdatedDetails("attributeName", "urn:ngsi-ld:Entity:01".toUri(), UpdateOperationResult.UPDATED)
                )
            )

        assertFalse(updateResult.isSuccessful())
    }

    @Test
    fun `it should find a failed update result if an attribute update has failed`() {
        val updateResult =
            UpdateResult(
                notUpdated = emptyList(),
                updated = listOf(
                    UpdatedDetails("attributeName", "urn:ngsi-ld:Entity:01".toUri(), UpdateOperationResult.UPDATED),
                    UpdatedDetails("attributeName", "urn:ngsi-ld:Entity:01".toUri(), UpdateOperationResult.FAILED)
                )
            )

        assertFalse(updateResult.isSuccessful())
    }
}
