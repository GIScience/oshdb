pipeline {
    agent {
        docker{
            image 'maven:3-jdk-8'
            args '-v /root/.m2:/root/.m2'
        }
    }
    
    stages {
        stage ('Build and Test') {
            steps {
              sh 'mvn clean compile javadoc:jar source:jar install' 
            }
          post{
            failure {
              rocketSend channel: 'jenkinsohsome', emoji: ':sob:' , message: 'Build failed. Review the code! ' , rawMessage: true
            }
          }
        }

    }
}
