#!/usr/bin/env groovy

def DOCKER_IMAGE = 'hub-adsoul.adsoul.com/adsoul/java-maven:11-3.6.0'

pipeline {
   agent any

   environment { // https://jenkins.io/doc/book/pipeline/syntax/#environment
      GIT_REPOSITORY_NAME = 'adsoul/redjob'
      GITHUB_API_TOKEN = credentials('github-api-token')

      BRANCH_NAME_URL = java.net.URLEncoder.encode(env.BRANCH_NAME, "UTF-8")
      JENKINS_BUILD_URL = "${env.JENKINS_URL}blue/organizations/jenkins/redjob/detail/${BRANCH_NAME_URL}/${env.BUILD_NUMBER}/pipeline"

      // https://docs.sonarqube.org/display/SONAR/Analysis+Parameters
      SONAR_URL = 'https://sonar.adsoul.com/'
      SONAR_API_TOKEN = credentials('godzilla-sonar-ci')
      SONAR_PROJECT_KEY = 'com.adsoul.redjob'
      SONAR_PROJECT_NAME = 'redjob'
   }

   options { // https://jenkins.io/doc/book/pipeline/syntax/#options
      ansiColor colorMapName: 'XTerm'
      buildDiscarder(logRotator(numToKeepStr: '5'))
      disableConcurrentBuilds()
      timeout(time: 30, unit: 'MINUTES')
      timestamps()
   }

   stages {
      stage('Build and Test') {
         agent {
            docker {
               image "${DOCKER_IMAGE}"
               // Map docker sockets for integration test containers.
               args '''
                  --net bridge
                  -v /etc/passwd:/etc/passwd
                  -v /etc/group:/etc/group
                  --group-add 999
                  -v /var/run/docker.sock:/var/run/docker.sock
                  -v ${HUDSON_HOME}/.m2/repository:/build/repository
                  -e HOME=/tmp
               '''
            }
         }
         steps {
            sh "mvn -Dfile.encoding=UTF-8 clean install"
         }
         post {
            always {
               junit allowEmptyResults: true, keepLongStdio: true, testResults: '**/target/surefire-reports/*.xml'
               archiveArtifacts artifacts: '**/target/*.jar', fingerprint: true
               echo '"Build and Test" stage finished.'
            }
         }
      }

      stage('SonarQube code quality') {
         agent {
            docker {
               image "${DOCKER_IMAGE}"
               args '''
                  -v ${HUDSON_HOME}/.m2/repository:/build/repository
                  -e HOME=/tmp
               '''
            }
         }
         when {
            branch 'master'
         }
         steps {
             sh '''
                # When branch plugin is installed, add: -Dsonar.branch.name=${BRANCH_NAME}
                mvn -Dsonar.host.url=${SONAR_URL} \
                    -Dsonar.login=${SONAR_API_TOKEN} \
                    -Dsonar.projectKey=${SONAR_PROJECT_KEY} \
                    -Dsonar.projectName=${SONAR_PROJECT_NAME} \
                    -Dsonar.github.repository=${GIT_REPOSITORY_NAME} \
                    -Dsonar.github.oauth=${GITHUB_API_TOKEN} \
                    -Dsonar.analysis.buildNumber=${BUILD_NUMBER} \
                    -Dsonar.analysis.revision=${CHANGE_ID} \
                    sonar:sonar
             '''
         }
      }
   }

   post { // https://jenkins.io/doc/book/pipeline/syntax/#post
      success {
         echo 'Build succeeded.'
      }

      unstable {
         echo 'Build unstable.'
         slackSend color: "warning", message: "${JOB_NAME}: Build unstable. See <${JENKINS_BUILD_URL}|Jenkins>"
      }

      failure {
         echo 'Build failed.'
         slackSend color: "danger", message: "${JOB_NAME}: Build failed. See <${JENKINS_BUILD_URL}|Jenkins>"
      }

      always {
         echo 'Build finished.'
      }
   }
}
