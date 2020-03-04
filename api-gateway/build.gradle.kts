dependencies {
	implementation("org.springframework.cloud:spring-cloud-starter-gateway")
	implementation("org.springframework.cloud:spring-cloud-starter-security")
	implementation("org.springframework.boot:spring-boot-starter-oauth2-client")
}

jib {
	from {
		image = "openjdk:alpine"
	}
	to {
		image = "easyglobalmarket/api-gateway"
	}
	container {
		jvmFlags = listOf("-Xms512m")
		ports = listOf("8080")
		creationTime = "USE_CURRENT_TIMESTAMP"
	}
}
