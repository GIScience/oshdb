pipeline {

/*tried slave dockers but only had disadvantages
    agent {
        docker{
            image 'maven:3-jdk-8'
            args '-v /root/.m2:/root/.m2'
        }
    }
*/

    agent any
    stages {
        stage ('Build and Test') {
            steps {
                script {
                    author = sh(returnStdout: true, script: 'git show -s --pretty=%an')
                }
                script {
                    server = Artifactory.server 'HeiGIT Repo'
                    rtMaven = Artifactory.newMavenBuild()
                    rtMaven.resolver server: server, releaseRepo: 'main', snapshotRepo: 'main'
                    rtMaven.deployer server: server, releaseRepo: 'libs-release-local', snapshotRepo: 'libs-snapshot-local'
                    rtMaven.deployer.addProperty("deployer", "jenkinsOhsome")
                    rtMaven.deployer.deployArtifacts = false
                    env.MAVEN_HOME = '/usr/share/maven'
                }
                script {
                    buildInfo = rtMaven.run pom: 'pom.xml', goals: 'clean compile javadoc:jar source:jar install'
                } 
            }
            post{
                failure {
                    rocketSend channel: 'jenkinsohsome', emoji: ':sob:' , message: "oshdb-build nr. ${env.BUILD_NUMBER} *failed* on Branch - ${env.BRANCH_NAME}  (<${env.BUILD_URL}|Open Build in Jenkins>). Latest commit from  ${author}. Review the code!" , rawMessage: true
                }
            }
        }

        stage ('deploy'){
            when {
                expression {
                    GIT_BRANCH = env.BRANCH_NAME
                    return GIT_BRANCH =~ /(^[0-9]+$)|(^(([0-9]+)(\\.))+([0-9]+)?$)|(^master$)/
                }
            }
            steps {
                script {
                    rtMaven.deployer.deployArtifacts buildInfo
                    server.publishBuildInfo buildInfo
                    BUILDNR=env.BUILD_NUMBER.toInteger()%10
                    echo BUILDNR
                    publishHTML([allowMissing: false, alwaysLinkToLastBuild: true, keepAll: false, reportDir: 'oshdb/target/apidocs', reportFiles: 'index.html', reportName: 'HTML Report', reportTitles: ''])


                }
            }
            post {
                failure {
                    rocketSend channel: 'jenkinsohsome', message: "Deployment of oshdb-build nr. ${env.BUILD_NUMBER} *failed* on Branch - ${env.BRANCH_NAME}  (<${env.BUILD_URL}|Open Build in Jenkins>). Latest commit from  ${author}. Is Artifactory running?" , rawMessage: true
                }
            }
        }

        stage ('encourage') {

            when { equals expected: 0  , actual: BUILDNR}
            steps {
                rocketSend channel: 'jenkinsohsome', message: "Happily deployed anther 10 oshdb-builds! Keep it up!" , rawMessage: true
            }
        }

    }
}
