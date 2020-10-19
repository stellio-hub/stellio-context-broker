package com.egm.stellio.subscription.web

import java.net.URI

fun subscriptionNotFoundMessage(subscriptionId: URI) = "Could not find a subscription with id $subscriptionId"
fun subscriptionAlreadyExistsMessage(subscriptionId: URI) = "A subscription with id $subscriptionId already exists"
fun subscriptionUnauthorizedMessage(subscriptionId: URI) =
    "User is not authorized to access subscription $subscriptionId"
