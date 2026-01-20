package com.egm.stellio.shared

import org.springframework.security.core.Authentication
import org.springframework.security.core.context.SecurityContext
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.oauth2.jwt.Jwt
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken
import org.springframework.security.test.context.support.WithSecurityContext
import org.springframework.security.test.context.support.WithSecurityContextFactory

@Retention(AnnotationRetention.RUNTIME)
@WithMockCustomUser(sub = "703f5572-a6fa-48a5-96dc-a800974869a3", roles = ["stellio-admin"])
annotation class WithMockAdminUser

@Retention(AnnotationRetention.RUNTIME)
@WithMockCustomUser(sub = "703f5572-a6fa-48a5-96dc-a800974869a3")
annotation class WithMockBasicUser

@Retention(AnnotationRetention.RUNTIME)
@WithSecurityContext(factory = WithMockCustomUserSecurityContextFactory::class)
annotation class WithMockCustomUser(
    val sub: String,
    val name: String = "Mock User",
    val groups: Array<String> = [],
    val roles: Array<String> = []
)

class WithMockCustomUserSecurityContextFactory : WithSecurityContextFactory<WithMockCustomUser> {
    override fun createSecurityContext(customUser: WithMockCustomUser): SecurityContext {
        val context: SecurityContext = SecurityContextHolder.createEmptyContext()
        val principal = Jwt.withTokenValue("token").header("alg", "none")
            .subject(customUser.sub)
            .claims {
                it["groups"] = customUser.groups.toList()
                it["realm_access"] = mapOf<String, Any>("roles" to customUser.roles.toList())
            }
            .build()
        val auth: Authentication = JwtAuthenticationToken(principal)
        auth.isAuthenticated = true
        context.authentication = auth
        return context
    }
}
