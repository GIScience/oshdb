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
                    def server = Artifactory.server 'HeiGIT Repo'
                    def rtMaven = Artifactory.newMavenBuild()
                    rtMaven.resolver server: server, releaseRepo: 'main', snapshotRepo: 'main'
                    rtMaven.deployer server: server, releaseRepo: 'libs-release-local', snapshotRepo: 'libs-snapshot-local'
                    rtMaven.deployer.addProperty("deployer", "jenkinsOhsome")
                    rtMaven.deployer.deployArtifacts = false
                    env.MAVEN_HOME = '/usr/share/maven/bin/mvn'
                }
                script {
                    def buildInfo = rtMaven.run pom: 'pom.xml', goals: 'clean compile javadoc:jar source:jar install'
                } 
            }
            post{
                failure {
                    script {
                        author = sh(returnStdout: true, script: 'git show -s --pretty=%an')
                    }
                    rocketSend channel: 'jenkinsohsome', emoji: ':sob:' , message: "Build Nr. ${env.BUILD_NUMBER} *failed* on Branch - ${env.BRANCH_NAME}  (<${env.BUILD_URL}|Open Build in Jenkins>). Latest commit from  ${author}. Review the code!" , rawMessage: true
                }
            }
        }

        stage ('deploy'){
            when { 
                expression {
                    return env.BRANCH_NAME == "(^[0-9]+$)|(^(([0-9]+)(\.))+([0-9]+)?$)|(^master$)"
                }
            }
            steps {
                script {
                    rtMaven.deployer.deployArtifacts buildInfo
                    server.publishBuildInfo buildInfo
                }
            }
        }

    }
}
