package com.egm.stellio.subscription.firebase

import com.google.auth.oauth2.GoogleCredentials
import com.google.firebase.FirebaseApp
import com.google.firebase.FirebaseOptions
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import javax.annotation.PostConstruct

@Service
class FCMInitializer {

    private val logger = LoggerFactory.getLogger(javaClass)

    @Value("\${google.application.credentials:}")
    private val credentials: String = ""

    @PostConstruct
    fun initialize() {
        if (credentials != "") {
            try {
                val options = FirebaseOptions.Builder()
                    .setCredentials(GoogleCredentials.getApplicationDefault())
                    .build()

                FirebaseApp.initializeApp(options)
                logger.info("Firebase application has been initialized")
            } catch (e: Exception) {
                logger.error(e.message)
            }
        }
    }
}