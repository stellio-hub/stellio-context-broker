package com.egm.stellio.subscription.repository
import com.egm.stellio.subscription.model.Subscription
import org.springframework.data.repository.reactive.ReactiveCrudRepository

import org.springframework.stereotype.Repository

@Repository
interface SubscriptionRepository : ReactiveCrudRepository<Subscription, String>