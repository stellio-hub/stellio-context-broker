package com.egm.stellio.shared.util

import com.apicatalog.jsonld.context.cache.Cache

class RemovableLruCache<K, V>(val maxCapacity: Int) : Cache<K, V> {

    companion object {
        const val LOAD_FACTOR = 0.75f
    }

    private val cache = object : LinkedHashMap<K, V>(maxCapacity, LOAD_FACTOR, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<K, V>) = size > maxCapacity
    }

    override fun containsKey(key: K): Boolean = cache.containsKey(key)

    override fun get(key: K): V? = cache[key]

    override fun put(key: K, value: V) {
        cache[key] = value
    }

    fun remove(key: K): V? = cache.remove(key)

    fun size(): Long = cache.size.toLong()
}
