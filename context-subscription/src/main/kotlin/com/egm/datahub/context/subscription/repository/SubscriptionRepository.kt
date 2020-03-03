package com.egm.datahub.context.subscription.repository
import com.egm.datahub.context.subscription.model.Subscription
import org.springframework.data.repository.reactive.ReactiveCrudRepository

import org.springframework.stereotype.Repository

@Repository
interface SubscriptionRepository : ReactiveCrudRepository<Subscription, String>