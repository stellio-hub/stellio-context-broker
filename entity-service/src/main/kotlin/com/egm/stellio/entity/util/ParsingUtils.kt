package com.egm.stellio.entity.util

import java.net.URLDecoder

fun extractComparaisonParametersFromQuery(query: String): ArrayList<String> {
    val splitted = ArrayList<String>()
    if (query.contains("==")) {
        splitted.add(query.split("==")[0])
        splitted.add("=")
        splitted.add(query.split("==")[1])
    } else if (query.contains("!=")) {
        splitted.add(query.split("!=")[0])
        splitted.add("<>")
        splitted.add(query.split("!=")[1])
    } else if (query.contains(">=")) {
        splitted.add(query.split(">=")[0])
        splitted.add(">=")
        splitted.add(query.split(">=")[1])
    } else if (query.contains(">")) {
        splitted.add(query.split(">")[0])
        splitted.add(">")
        splitted.add(query.split(">")[1])
    } else if (query.contains("<=")) {
        splitted.add(query.split("<=")[0])
        splitted.add("<=")
        splitted.add(query.split("<=")[1])
    } else if (query.contains("<")) {
        splitted.add(query.split("<")[0])
        splitted.add("<")
        splitted.add(query.split("<")[1])
    }
    return splitted
}

fun String.decode(): String =
    URLDecoder.decode(this, "UTF-8")

fun List<String>.decode(): List<String> =
    this.map {
        it.decode()
    }
