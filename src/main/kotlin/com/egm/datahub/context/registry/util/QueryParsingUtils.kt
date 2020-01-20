package com.egm.datahub.context.registry.util

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