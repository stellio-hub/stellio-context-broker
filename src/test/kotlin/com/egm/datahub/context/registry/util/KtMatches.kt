package com.egm.datahub.context.registry.util

import org.hamcrest.Description
import org.hamcrest.Matcher
import org.hamcrest.TypeSafeMatcher

class KtMatches(private val regex: Regex) : TypeSafeMatcher<String>() {

    override fun matchesSafely(input: String): Boolean {
        return regex.matches(input.replace(" ", ""))
    }

    override fun describeTo(description: Description) {
        description.appendText("matches the given input string")
    }

    companion object {
        fun ktMatches(regex: String): Matcher<String> {
            return KtMatches(regex.replace("\n", "").replace(" ", "").toRegex())
        }
    }
}