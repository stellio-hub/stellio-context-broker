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
                slackSend (color: '#D4DADF', message: "Started ${env.BUILD_URL}")
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
                anyOf {
                    changeset "entity-service/**"
                    changeset "shared/**"
                }
            }
            steps {
                sh './gradlew build -p entity-service'
            }
        }
        stage('Build Subscription Service') {
            when {
                anyOf {
                    changeset "subscription-service/**"
                    changeset "shared/**"
                }
            }
            steps {
                sh './gradlew build -p subscription-service'
            }
        }
        stage('Build Search Service') {
            when {
                anyOf {
                    changeset "search-service/**"
                    changeset "shared/**"
                }
            }
            steps {
                sh './gradlew build -p search-service'
            }
        }
        stage('Dockerize Api Gateway') {
            when {
                branch 'develop'
                changeset "api-gateway/**"
            }
            steps {
                sh './gradlew jib -Djib.to.auth.username=$JIB_CREDS_USR -Djib.to.auth.password=$JIB_CREDS_PSW -p api-gateway'
            }
        }
        stage('Dockerize Entity Service') {
            when {
                branch 'develop'
                anyOf {
                    changeset "entity-service/**"
                    changeset "shared/**"
                }
            }
            steps {
                sh './gradlew jib -Djib.to.auth.username=$JIB_CREDS_USR -Djib.to.auth.password=$JIB_CREDS_PSW -p entity-service'
            }
        }
        stage('Dockerize Subscription Service') {
            when {
                branch 'develop'
                anyOf {
                    changeset "subscription-service/**"
                    changeset "shared/**"
                }
            }
            steps {
                sh './gradlew jib -Djib.to.auth.username=$JIB_CREDS_USR -Djib.to.auth.password=$JIB_CREDS_PSW -p subscription-service'
            }
        }
        stage('Dockerize Search Service') {
            when {
                branch 'develop'
                anyOf {
                    changeset "search-service/**"
                    changeset "shared/**"
                }
            }
            steps {
                sh './gradlew jib -Djib.to.auth.username=$JIB_CREDS_USR -Djib.to.auth.password=$JIB_CREDS_PSW -p search-service'
            }
        }
        stage('Deploy - Integration') {
            when {
                branch 'develop'
            }
            steps {
                build job: '../DataHub.Int.Launcher'
            }
        }
    }
    post {
        always {
            archiveArtifacts artifacts: '**/build/reports/**', allowEmptyArchive: true
        }
        success {
            slackSend (color: '#36b37e', message: "Success: ${env.BUILD_URL} after ${currentBuild.durationString.replace(' and counting', '')}")
        }
        failure {
            slackSend (color: '#FF0000', message: "Fail: ${env.BUILD_URL} after ${currentBuild.durationString.replace(' and counting', '')}")
        }
    }
}
