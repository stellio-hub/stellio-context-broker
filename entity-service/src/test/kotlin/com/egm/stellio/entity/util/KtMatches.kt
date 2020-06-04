package com.egm.stellio.entity.util

import org.hamcrest.Description
import org.hamcrest.Matcher
import org.hamcrest.TypeSafeMatcher

class KtMatches(private val regex: Regex) : TypeSafeMatcher<String>() {

    override fun matchesSafely(input: String): Boolean {
        val concatInput = input.replace("\n", "").replace(" ", "")
        return regex.matches(concatInput)
    }

    override fun describeTo(description: Description) {
        description.appendText("matches the regex : $regex")
    }

    companion object {
        fun ktMatches(regex: String): Matcher<String> {
            return KtMatches(regex.replace("\n", "").replace(" ", "").toRegex())
        }
    }
}
