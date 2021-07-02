package com.egm.stellio.entity.repository

import com.egm.stellio.shared.model.QueryParams
import org.springframework.transaction.annotation.Transactional
import java.net.URI

interface SearchRepository {

    /**
     * Searches the requested entities and applies permissions checks in authentication enabled mode.
     *
     * @param queryParams query parameters.
     * @param userSub to be used in authentication enabled mode to apply permissions checks.
     * @param page page number for pagination.
     * @param limit limit number for pagination.
     * @param contexts list of JSON-LD contexts for term to URI expansion.
     * @return [Pair]
     *  @property first count of all matching entities in the database.
     *  @property second list of matching entities ids as requested by pagination sorted by entity id.
     */
    @Transactional(readOnly = true)
    fun getEntities(
        queryParams: QueryParams,
        userSub: String,
        page: Int,
        limit: Int,
        contexts: List<String>
    ): Pair<Int, List<URI>>
}
