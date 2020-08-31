pipeline {
    agent any
    tools {
        jdk 'JDK 11'
    }
    environment {
        EGM_CI_DH = credentials('egm-ci-dh')
    }
    stages {
        stage('Pre Build') {
            steps {
                slackSend (color: '#D4DADF', message: "Started ${env.BUILD_URL}")
            }
        }
        stage('Build Shared Lib') {
            when {
                changeset "shared/**"
            }
            steps {
                sh './gradlew build -p shared'
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
        /* Jib only allows to add tags and always set the "latest" tag on the Docker images created.
        It's unavoidable to create separate stages for Dockerizing dev services and specify the full to.image path */
        stage('Dockerize Dev Api Gateway') {
            when {
                branch 'develop'
                changeset "api-gateway/**"
            }
            steps {
                sh './gradlew jib -Djib.to.image=stellio/stellio-api-gateway:dev -Djib.to.auth.username=$EGM_CI_DH_USR -Djib.to.auth.password=$EGM_CI_DH_PSW -p api-gateway'
            }
        }
        stage('Dockerize Api Gateway') {
            when {
                branch 'master'
                changeset "api-gateway/**"
            }
            steps {
                sh './gradlew jib -Djib.to.auth.username=$EGM_CI_DH_USR -Djib.to.auth.password=$EGM_CI_DH_PSW -p api-gateway'
            }
        }
        stage('Dockerize Dev Entity Service') {
            when {
                branch 'develop'
                anyOf {
                    changeset "entity-service/**"
                    changeset "shared/**"
                }
            }
            steps {
                sh './gradlew jib -Djib.to.image=stellio/stellio-entity-service:dev -Djib.to.auth.username=$EGM_CI_DH_USR -Djib.to.auth.password=$EGM_CI_DH_PSW -p entity-service'
            }
        }
        stage('Dockerize Entity Service') {
            when {
                branch 'master'
                anyOf {
                    changeset "entity-service/**"
                    changeset "shared/**"
                }
            }
            steps {
                sh './gradlew jib -Djib.to.auth.username=$EGM_CI_DH_USR -Djib.to.auth.password=$EGM_CI_DH_PSW -p entity-service'
            }
        }
        stage('Dockerize Dev Subscription Service') {
            when {
                branch 'develop'
                anyOf {
                    changeset "subscription-service/**"
                    changeset "shared/**"
                }
            }
            steps {
                sh './gradlew jib -Djib.to.image=stellio/stellio-subscription-service:dev -Djib.to.auth.username=$EGM_CI_DH_USR -Djib.to.auth.password=$EGM_CI_DH_PSW -p subscription-service'
            }
        }
        stage('Dockerize Subscription Service') {
            when {
                branch 'master'
                anyOf {
                    changeset "subscription-service/**"
                    changeset "shared/**"
                }
            }
            steps {
                sh './gradlew jib -Djib.to.auth.username=$EGM_CI_DH_USR -Djib.to.auth.password=$EGM_CI_DH_PSW -p subscription-service'
            }
        }
        stage('Dockerize Dev Search Service') {
            when {
                branch 'develop'
                anyOf {
                    changeset "search-service/**"
                    changeset "shared/**"
                }
            }
            steps {
                sh './gradlew jib -Djib.to.image=stellio/stellio-search-service:dev -Djib.to.auth.username=$EGM_CI_DH_USR -Djib.to.auth.password=$EGM_CI_DH_PSW -p search-service'
            }
        }
        stage('Dockerize Search Service') {
            when {
                branch 'master'
                anyOf {
                    changeset "search-service/**"
                    changeset "shared/**"
                }
            }
            steps {
                sh './gradlew jib -Djib.to.auth.username=$EGM_CI_DH_USR -Djib.to.auth.password=$EGM_CI_DH_PSW -p search-service'
            }
        }
    }
    post {
        always {
            archiveArtifacts artifacts: '**/build/reports/**', allowEmptyArchive: true

            recordIssues enabledForFailure: true, tool: checkStyle(pattern: '**/build/reports/ktlint/ktlint*.xml')
            recordIssues enabledForFailure: true, tool: detekt(pattern: '**/build/reports/detekt/detekt.xml')
        }
        success {
            script {
                if (env.BRANCH_NAME == 'master')
                    build job: '../DataHub.Int.Launcher'
                else if (env.BRANCH_NAME == 'develop')
                    build job: '../DataHub.Api-Tests.Launcher'
            }
            slackSend (color: '#36b37e', message: "Success: ${env.BUILD_URL} after ${currentBuild.durationString.replace(' and counting', '')}")
        }
        failure {
            slackSend (color: '#FF0000', message: "Fail: ${env.BUILD_URL} after ${currentBuild.durationString.replace(' and counting', '')}")
        }
    }
}
