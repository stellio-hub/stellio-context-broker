package com.egm.stellio.search.entity.model

import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class UpdateResultTests {

    @Test
    fun `it should find the successful update operation results`() {
        assertTrue(OperationStatus.UPDATED.isSuccessResult())
        assertTrue(OperationStatus.APPENDED.isSuccessResult())
        assertTrue(OperationStatus.REPLACED.isSuccessResult())
        assertTrue(OperationStatus.IGNORED.isSuccessResult())
    }

    @Test
    fun `it should find the failed update operation results`() {
        assertFalse(OperationStatus.FAILED.isSuccessResult())
    }

    @Test
    fun `it should find a successful update result`() {
        val updateResult =
            UpdateResult(
                notUpdated = emptyList(),
                updated = listOf(
                    UpdatedDetails("attributeName", "urn:ngsi-ld:Entity:01".toUri(), OperationStatus.UPDATED)
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
                    UpdatedDetails("attributeName", "urn:ngsi-ld:Entity:01".toUri(), OperationStatus.UPDATED)
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
                    UpdatedDetails("attributeName", "urn:ngsi-ld:Entity:01".toUri(), OperationStatus.UPDATED),
                    UpdatedDetails("attributeName", "urn:ngsi-ld:Entity:01".toUri(), OperationStatus.FAILED)
                )
            )

        assertFalse(updateResult.isSuccessful())
    }
}
