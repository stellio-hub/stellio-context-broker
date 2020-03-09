pipeline {
    agent any
    tools {
        jdk 'JDK 11'
    }
    environment {
        JIB_CREDS = credentials('jib-creds')
    }
    stages {
        stage('Pre Build') {
            steps {
                slackSend (color: '#D4DADF', message: "Started ${env.BUILD_TAG}")
            }
        }
        stage('Build Api Gateway') {
            when {
                changeset "api-gateway/**"
            }
            steps {
                sh './gradlew build -p api-gateway'
            }
        }
        stage('Build Entity Service') {
            when {
                changeset "entity-service/**"
            }
            steps {
                sh './gradlew build -p entity-service'
            }
        }
        stage('Build Subscription Service') {
            when {
                changeset "subscription-service/**"
            }
            steps {
                sh './gradlew build -p subscription-service'
            }
        }
        stage('Build Search Service') {
            when {
                changeset "search-service/**"
            }
            steps {
                sh './gradlew build -p search-service'
            }
        }
        stage('Deploy Api Gateway - Integration') {
            when {
                branch 'develop'
                changeset "api-gateway/**"
            }
            steps {
                sh './gradlew jib -Djib.to.auth.username=$JIB_CREDS_USR -Djib.to.auth.password=$JIB_CREDS_PSW -p api-gateway'
            }
        }
        stage('Deploy Entity Service - Integration') {
            when {
                branch 'develop'
                changeset "entity-service/**"
            }
            steps {
                sh './gradlew jib -Djib.to.auth.username=$JIB_CREDS_USR -Djib.to.auth.password=$JIB_CREDS_PSW -p entity-service'
            }
        }
        stage('Deploy Subscription Service - Integration') {
            when {
                branch 'develop'
                changeset "subscription-service/**"
            }
            steps {
                sh './gradlew jib -Djib.to.auth.username=$JIB_CREDS_USR -Djib.to.auth.password=$JIB_CREDS_PSW -p subscription-service'
            }
        }
        stage('Deploy Search Service - Integration') {
            when {
                branch 'develop'
                changeset "search-service/**"
            }
            steps {
                sh './gradlew jib -Djib.to.auth.username=$JIB_CREDS_USR -Djib.to.auth.password=$JIB_CREDS_PSW -p search-service'
            }
        }
    }
    post {
        always {
            archiveArtifacts artifacts: '**/build/reports/**', allowEmptyArchive: true
        }
        success {
            slackSend (color: '#36b37e', message: "Success: ${env.BUILD_TAG} after ${currentBuild.durationString.replace(' and counting', '')}")
        }
        failure {
            slackSend (color: '#FF0000', message: "Fail: ${env.BUILD_TAG} after ${currentBuild.durationString.replace(' and counting', '')}")
        }
    }
}