pipeline {
    agent any    
    stages {
        stage ('Build and Test') {
            steps {
              sh 'mvn clean compile javadoc:jar source:jar install' 
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

    }
}
