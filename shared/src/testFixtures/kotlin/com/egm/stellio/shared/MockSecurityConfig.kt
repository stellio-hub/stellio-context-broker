package com.egm.stellio.shared

import org.springframework.security.core.Authentication
import org.springframework.security.core.context.SecurityContext
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.oauth2.jwt.Jwt
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken
import org.springframework.security.test.context.support.WithSecurityContext
import org.springframework.security.test.context.support.WithSecurityContextFactory

@Retention(AnnotationRetention.RUNTIME)
@WithSecurityContext(factory = WithMockCustomUserSecurityContextFactory::class)
annotation class WithMockCustomUser(val sub: String, val name: String)

class WithMockCustomUserSecurityContextFactory : WithSecurityContextFactory<WithMockCustomUser> {
    override fun createSecurityContext(customUser: WithMockCustomUser): SecurityContext {
        val context: SecurityContext = SecurityContextHolder.createEmptyContext()
        val principal = Jwt.withTokenValue("token").header("alg", "none").subject(customUser.sub).build()
        val auth: Authentication = JwtAuthenticationToken(principal)
        auth.isAuthenticated = true
        context.authentication = auth
        return context
    }
}
