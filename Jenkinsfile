pipeline {
    agent any
    tools {
        jdk 'JDK 17'
    }
    environment {
        EGM_CI_DH = credentials('egm-ci-dh')
    }
    stages {
        stage('Notify build in Slack') {
            steps {
                slackSend (color: '#D4DADF', message: "Starting: Stellio on branch ${env.BRANCH_NAME} (<${env.BUILD_URL}|Open>)")
            }
        }
        stage('Clean previous build') {
            steps {
                sh './gradlew clean'
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
        /* Publish images for develop and master branches, using version defined in build.gradle.kts
        For develop branch, produce an additional latest-dev tag */
        stage('Dockerize Services') {
            when {
                anyOf {
                    branch 'develop'
                    branch 'master'
                }
            }
            steps {
                sh './gradlew jib -Djib.to.auth.username=$EGM_CI_DH_USR -Djib.to.auth.password=$EGM_CI_DH_PSW'
            }
        }
        stage('Dockerize Latest Dev Services') {
            when {
                branch 'develop'
            }
            steps {
                sh './gradlew jib -Djib.to.tags=latest-dev -Djib.to.auth.username=$EGM_CI_DH_USR -Djib.to.auth.password=$EGM_CI_DH_PSW'
            }
        }
        /* Used when we want to publish images for a specific tag (e.g., FIWARE versions) */
        stage('Build tagger Docker images') {
            steps {
                script {
                    def currentTags = sh(returnStdout: true, script: "git tag --points-at | tr -d \" *\" | xargs").trim().split(" ")
                    if (currentTags.size() > 0 && currentTags[0].trim() != "") {
                        for (int i = 0; i < currentTags.size(); ++i) {
                            env.CURRENT_TAG = currentTags[i]
                            sh './gradlew jib -Djib.to.image=stellio/stellio-api-gateway:$CURRENT_TAG -Djib.to.auth.username=$EGM_CI_DH_USR -Djib.to.auth.password=$EGM_CI_DH_PSW -p api-gateway'
                            sh './gradlew jib -Djib.to.image=stellio/stellio-search-service:$CURRENT_TAG -Djib.to.auth.username=$EGM_CI_DH_USR -Djib.to.auth.password=$EGM_CI_DH_PSW -p search-service'
                            sh './gradlew jib -Djib.to.image=stellio/stellio-subscription-service:$CURRENT_TAG -Djib.to.auth.username=$EGM_CI_DH_USR -Djib.to.auth.password=$EGM_CI_DH_PSW -p subscription-service'
                        }
                    }
                }
            }
        }
    }
    post {
        always {
            archiveArtifacts artifacts: '**/build/reports/**', allowEmptyArchive: true

            junit allowEmptyResults: true, keepLongStdio: true, testResults: '**/build/test-results/test/*.xml'
            recordIssues enabledForFailure: true, tool: detekt(pattern: '**/build/reports/detekt/detekt.xml')
        }
        success {
            script {
                if (env.BRANCH_NAME == 'master')
                    build job: '../DataHub.Int.Launcher'
                else if (env.BRANCH_NAME == 'develop')
                    build job: '../DataHub.NGSI-LD-Test-Suite.Launcher'
            }
            slackSend (color: '#36b37e', message: "Success: Stellio on branch ${env.BRANCH_NAME} after ${currentBuild.durationString.replace(' and counting', '')} (<${env.BUILD_URL}|Open>)")
        }
        failure {
            slackSend (color: '#FF0000', message: "Fail: Stellio on branch ${env.BRANCH_NAME} after ${currentBuild.durationString.replace(' and counting', '')} (<${env.BUILD_URL}|Open>)")
        }
    }
}
