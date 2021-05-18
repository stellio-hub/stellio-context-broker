package com.egm.stellio.entity.repository

import java.net.URI

interface SearchRepository {

    fun getEntities(params: Map<String, Any?>, userId: String, page: Int, limit: Int): Pair<Int, List<URI>>
}
