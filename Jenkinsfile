pipeline {
  agent {
    label 'worker'
    }
    tools {
      maven 'Maven 3'
    }

  environment {
    SNAPSHOT_BRANCH_REGEX = /(^master$)/
    // START CUSTOM oshdb
    BENCHMARK_BRANCH_REGEX = /(^master$)/
    // END CUSTOM oshdb
    RELEASE_REGEX = /^([0-9]+(\.[0-9]+)*)(-(RC|beta-|alpha-)[0-9]+)?$/
    RELEASE_DEPLOY = false
    SNAPSHOT_DEPLOY = false
  }

  stages {

    stage ("test ls") {
      steps {
        script {
          sh "tree /jenkins"
        }
      }
    }

    // stage ('Build and Test') {
    //   steps {

    //     setup_basic_env()

    //     mavenbuild('clean compile verify javadoc:jar source:jar -P jacoco,sign,git') //javadoc:jar source:jar  
    //   }
    //   post {
    //     failure {
    //       rocket_buildfail()
    //     }
    //   }
    // }

    // stage ('Reports and Statistics') {
    //   steps {
    //     script {
    //       withSonarQubeEnv('sonarcloud GIScience/ohsome') {
    //         // START CUSTOM oshdb
    //         SONAR_CLI_PARAMETER = "-Dsonar.projectName=OSHDB"
    //         // END CUSTOM oshdb
    //         if (env.CHANGE_ID) {
    //           SONAR_CLI_PARAMETER += " " +
    //             "-Dsonar.pullrequest.key=${env.CHANGE_ID} " +
    //             "-Dsonar.pullrequest.branch=${env.CHANGE_BRANCH} " +
    //             "-Dsonar.pullrequest.base=${env.CHANGE_TARGET}"
    //         } else {
    //           SONAR_CLI_PARAMETER += " " +
    //             "-Dsonar.branch.name=${env.BRANCH_NAME}"
    //         }
    //         sh "mvn --batch-mode --update-snapshots sonar:sonar ${SONAR_CLI_PARAMETER}"
    //       }
    //       echo "${REPO_NAME}"
    //       echo "${SONAR_CLI_PARAMETER}"
    //       echo "${VERSION}"
    //       echo "${env.BRANCH_NAME}"
    //       echo "${env.BUILD_NUMBER}"
    //       echo "${LATEST_COMMIT_ID}"
    //       report_basedir = "/jenkins/reports/${REPO_NAME}/${VERSION}_${env.BRANCH_NAME}/${env.BUILD_NUMBER}_${LATEST_COMMIT_ID}"

    //       // jacoco
    //       report_dir = report_basedir + "/jacoco/"

    //       jacoco(
    //           execPattern      : '**/target/jacoco.exec',
    //           classPattern     : '**/target/classes',
    //           sourcePattern    : '**/src/main/java',
    //           inclusionPattern : 'org/heigit/**'
    //       )


    //       // sh "mkdir -p ${report_dir} && rm -Rf ${report_dir}* && find . -path '*/target/site/jacoco' -exec cp -R --parents {} ${report_dir} \\; && find ${report_dir} -path '*/target/site/jacoco' | while read line; do echo \$line; neu=\${line/target\\/site\\/jacoco/} ;  mv \$line/* \$neu ; done && find ${report_dir} -type d -empty -delete"

    //       // // warnings plugin
    //       // // START CUSTOM oshdb
    //       // // CUSTOM: use test-compile
    //       // rtMaven.run pom: 'pom.xml', goals: '$MAVEN_GENERAL_OPTIONS -V -e test-compile checkstyle:checkstyle pmd:pmd pmd:cpd spotbugs:spotbugs -Dmaven.repo.local=.m2 $MAVEN_TEST_OPTIONS'
    //       // // END CUSTOM oshdb

    //       // recordIssues enabledForFailure: true, tools: [mavenConsole(),  java(), javaDoc()]
    //       // recordIssues enabledForFailure: true, tool: checkStyle()
    //       // recordIssues enabledForFailure: true, tool: spotBugs()
    //       // recordIssues enabledForFailure: true, tool: cpd(pattern: '**/target/cpd.xml')
    //       // recordIssues enabledForFailure: true, tool: pmdParser(pattern: '**/target/pmd.xml')
    //     }
    //   }
    // //   post {
    // //     failure {
    // //       rocketSend channel: 'jenkinsohsome', emoji: ':disappointed:', message: "Reporting of *${REPO_NAME}*-build nr. ${env.BUILD_NUMBER} *failed* on Branch - ${env.BRANCH_NAME}  (<${env.BUILD_URL}|Open Build in Jenkins>). Latest commit from  ${LATEST_AUTHOR}." , rawMessage: true
    // //     }
    // //   }
    // }

    // stage ('Deploy Snapshot') {
    //   when {
    //     expression {
    //       return env.BRANCH_NAME ==~ SNAPSHOT_BRANCH_REGEX && VERSION ==~ /.*-SNAPSHOT$/
    //     }
    //   }
    //   steps {
    //     script {
    //       // START CUSTOM oshdb
    //       // CUSTOM: added withDep profile
    //       withCredentials([string(credentialsId: 'gpg-signing-key-passphrase', variable: 'PASSPHRASE')]) {
    //         buildInfo = rtMaven.run pom: 'pom.xml', goals: '$MAVEN_GENERAL_OPTIONS clean compile javadoc:jar source:jar install -P sign,git,withDep -Dmaven.repo.local=.m2 $MAVEN_TEST_OPTIONS -Dgpg.passphrase=$PASSPHRASE -DskipTests=true'
    //       }
    //       // END CUSTOM oshdb
    //       rtMaven.deployer.deployArtifacts buildInfo
    //       server.publishBuildInfo buildInfo
    //       SNAPSHOT_DEPLOY = true
    //     }
    //   }
    //   post {
    //     failure {
    //       rocketSend channel: 'jenkinsohsome', emoji: ':disappointed:', message: "Deployment of *${REPO_NAME}*-build nr. ${env.BUILD_NUMBER} *failed* on Branch - ${env.BRANCH_NAME}  (<${env.BUILD_URL}|Open Build in Jenkins>). Latest commit from  ${LATEST_AUTHOR}. Is Artifactory running?" , rawMessage: true
    //     }
    //   }
    // }

    // stage ('Deploy Release') {
    //   when {
    //     expression {
    //       return VERSION ==~ RELEASE_REGEX && env.TAG_NAME ==~ RELEASE_REGEX
    //     }
    //   }
    //   steps {
    //     script {
    //       // START CUSTOM oshdb
    //       // CUSTOM: added withDep profile
    //       withCredentials([string(credentialsId: 'gpg-signing-key-passphrase', variable: 'PASSPHRASE')]) {
    //         buildInfo = rtMaven.run pom: 'pom.xml', goals: '$MAVEN_GENERAL_OPTIONS clean compile javadoc:jar source:jar install -P sign,git,withDep -Dmaven.repo.local=.m2 $MAVEN_TEST_OPTIONS -Dgpg.passphrase=$PASSPHRASE -DskipTests=true'
    //       }
    //       // END CUSTOM oshdb
    //       rtMaven.deployer.deployArtifacts buildInfo
    //       server.publishBuildInfo buildInfo
    //       RELEASE_DEPLOY = true
    //     }
    //     withCredentials([
    //         file(credentialsId: 'ossrh-settings', variable: 'settingsFile'),
    //         string(credentialsId: 'gpg-signing-key-passphrase', variable: 'PASSPHRASE')
    //     ]) {
    //       sh 'mvn $MAVEN_GENERAL_OPTIONS clean compile -s $settingsFile javadoc:jar source:jar deploy -P sign,git,deploy-central -Dmaven.repo.local=.m2 $MAVEN_TEST_OPTIONS -Dgpg.passphrase=$PASSPHRASE -DskipTests=true'
    //     }
    //   }
    //   post {
    //     failure {
    //       rocketSend channel: 'jenkinsohsome', emoji: ':disappointed:', message: "Deployment of *${REPO_NAME}*-build nr. ${env.BUILD_NUMBER} *failed* on Branch - ${env.BRANCH_NAME}  (<${env.BUILD_URL}|Open Build in Jenkins>). Latest commit from  ${LATEST_AUTHOR}. Is Artifactory running?" , rawMessage: true
    //     }
    //   }
    // }

    // // START CUSTOM oshdb
    // stage ('Trigger Benchmarks') {
    //   when {
    //     expression {
    //       return env.BRANCH_NAME ==~ BENCHMARK_BRANCH_REGEX
    //     }
    //   }
    //   steps {
    //     build job: 'oshdb-benchmark/master', quietPeriod: 360, wait: false
    //   }
    //   post {
    //     failure {
    //       rocketSend channel: 'jenkinsohsome', emoji: ':disappointed:', message: "Triggering of Benchmarks for ${REPO_NAME}-build nr. ${env.BUILD_NUMBER} *failed* on Branch - ${env.BRANCH_NAME}  (<${env.BUILD_URL}|Open Build in Jenkins>). Does the benchmark job still exist?" , rawMessage: true
    //     }
    //   }
    // }

    // stage ('Build Examples') {
    //   when {
    //     anyOf {
    //       equals expected: true, actual: RELEASE_DEPLOY
    //       equals expected: true, actual: SNAPSHOT_DEPLOY
    //     }
    //   }
    //   steps {
    //     script {
    //       if (RELEASE_DEPLOY == true) {
    //         build job: 'oshdb-examples/oshdb-stable', quietPeriod: 360, wait: false
    //       } else {
    //         build job: 'oshdb-examples/oshdb-snapshot', quietPeriod: 360, wait: false
    //       }
    //     }
    //   }
    //   post {
    //     failure {
    //       rocketSend channel: 'jenkinsohsome', emoji: ':disappointed:', message: "Triggering of Examples build for ${REPO_NAME}-build nr. ${env.BUILD_NUMBER} *failed* on Branch - ${env.BRANCH_NAME}  (<${env.BUILD_URL}|Open Build in Jenkins>)." , rawMessage: true
    //     }
    //   }
    // }
    // // END CUSTOM oshdb

    // stage ('Publish Javadoc') {
    //   when {
    //     anyOf {
    //       equals expected: true, actual: RELEASE_DEPLOY
    //       equals expected: true, actual: SNAPSHOT_DEPLOY
    //     }
    //   }
    //   steps {
    //     script {
    //       // load dependencies to artifactory
    //       rtMaven.run pom: 'pom.xml', goals: '$MAVEN_GENERAL_OPTIONS org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version -Dmaven.repo.local=.m2 $MAVEN_TEST_OPTIONS'

    //       javadc_dir = "/srv/javadoc/java/" + REPO_NAME + "/" + VERSION + "/"
    //       echo javadc_dir

    //       rtMaven.run pom: 'pom.xml', goals: '$MAVEN_GENERAL_OPTIONS clean javadoc:javadoc -Dmaven.repo.local=.m2 $MAVEN_TEST_OPTIONS'
    //       sh "echo ${javadc_dir}"
    //       // make sure jenkins uses bash not dash!
    //       sh "mkdir -p ${javadc_dir} && rm -Rf ${javadc_dir}* && find . -path '*/target/site/apidocs' -exec cp -R --parents {} ${javadc_dir} \\; && find ${javadc_dir} -path '*/target/site/apidocs' | while read line; do echo \$line; neu=\${line/target\\/site\\/apidocs/} ;  mv \$line/* \$neu ; done && find ${javadc_dir} -type d -empty -delete"
    //     }

    //     // START CUSTOM oshdb
    //     script {
    //       javadc_dir = javadc_dir + "aggregated/"
    //       rtMaven.run pom: 'pom.xml', goals: '$MAVEN_GENERAL_OPTIONS --update-snapshots clean javadoc:aggregate -Dmaven.repo.local=.m2 $MAVEN_TEST_OPTIONS'
    //       sh "mkdir -p ${javadc_dir} && rm -Rf ${javadc_dir}* && find . -path './target/site/apidocs' -exec cp -R --parents {} ${javadc_dir} \\; && find ${javadc_dir} -path '*/target/site/apidocs' | while read line; do echo \$line; neu=\${line/target\\/site\\/apidocs/} ;  mv \$line/* \$neu ; done && find ${javadc_dir} -type d -empty -delete"
    //     }
    //     // END CUSTOM oshdb
    //   }
    //   post {
    //     failure {
    //       rocketSend channel: 'jenkinsohsome', emoji: ':disappointed:', message: "Deployment of javadoc *${REPO_NAME}*-build nr. ${env.BUILD_NUMBER} *failed* on Branch - ${env.BRANCH_NAME}  (<${env.BUILD_URL}|Open Build in Jenkins>). Latest commit from  ${LATEST_AUTHOR}." , rawMessage: true
    //     }
    //   }
    // }

    // stage ('Check Dependencies') {
    //   when {
    //     expression {
    //       if ((currentBuild.number > 1) && (env.BRANCH_NAME ==~ SNAPSHOT_BRANCH_REGEX)) {
    //         month_pre = new Date(currentBuild.previousBuild.rawBuild.getStartTimeInMillis())[Calendar.MONTH]
    //         echo month_pre.toString()
    //         month_now = new Date(currentBuild.rawBuild.getStartTimeInMillis())[Calendar.MONTH]
    //         echo month_now.toString()
    //         return month_pre != month_now
    //       }
    //       return false
    //     }
    //   }
    //   steps {
    //     script {
    //       try {
    //         update_notify = sh(returnStdout: true, script: 'mvn $MAVEN_GENERAL_OPTIONS versions:display-dependency-updates | grep -Pzo "(?s)The following dependencies([^\\n]*\\S\\n)*[^\\n]*\\s\\n"').trim()
    //         echo update_notify
    //         rocketSend channel: 'jenkinsohsome', emoji: ':wave:' , message: "Check your dependencies in *${REPO_NAME}*. You might have updates: ${update_notify}" , rawMessage: true
    //       } catch (err) {
    //         echo "No maven dependency upgrades found."
    //       }
    //     }
    //     script {
    //       try {
    //         update_notify = sh(returnStdout: true, script: 'mvn $MAVEN_GENERAL_OPTIONS versions:display-plugin-updates | grep -Pzo "(?s)The following plugin update([^\\n]*\\S\\n)*[^\\n]*\\s\\n"').trim()
    //         echo update_notify
    //         rocketSend channel: 'jenkinsohsome', emoji: ':wave:' , message: "Check your maven plugins in *${REPO_NAME}*. You might have updates: ${update_notify}" , rawMessage: true
    //       } catch (err) {
    //         echo "No maven plugin upgrades found."
    //       }
    //     }
    //   }
    // }

    // stage ('Encourage') {
    //   when {
    //     expression {
    //       if (currentBuild.number > 1) {
    //         date_pre = new Date(currentBuild.previousBuild.rawBuild.getStartTimeInMillis()).clearTime()
    //         echo date_pre.format( 'yyyyMMdd' )
    //         date_now = new Date(currentBuild.rawBuild.getStartTimeInMillis()).clearTime()
    //         echo date_now.format( 'yyyyMMdd' )
    //         return date_pre.numberAwareCompareTo(date_now) < 0
    //       }
    //       return false
    //     }
    //   }
    //   steps {
    //     rocketSend channel: 'jenkinsohsome', emoji: ':wink:', message: "Hey, this is just your daily notice that Jenkins is still working for you on *${REPO_NAME}* Branch ${env.BRANCH_NAME}! Happy and for free! Keep it up!" , rawMessage: true
    //   }
    //   post {
    //     failure {
    //       rocketSend channel: 'jenkinsohsome', emoji: ':disappointed:', message: "Reporting of *${REPO_NAME}*-build nr. ${env.BUILD_NUMBER} *failed* on Branch - ${env.BRANCH_NAME}  (<${env.BUILD_URL}|Open Build in Jenkins>). Latest commit from  ${LATEST_AUTHOR}." , rawMessage: true
    //     }
    //   }
    // }

    // stage ('Report Status Change') {
    //   when {
    //     expression {
    //       return ((currentBuild.number > 1) && (currentBuild.getPreviousBuild().result == 'FAILURE'))
    //     }
    //   }
    //   steps {
    //     rocketSend channel: 'jenkinsohsome', emoji: ':sunglasses:', message: "We had some problems, but we are BACK TO NORMAL! Nice debugging: *${REPO_NAME}*-build-nr. ${env.BUILD_NUMBER} *succeeded* on Branch - ${env.BRANCH_NAME}  (<${env.BUILD_URL}|Open Build in Jenkins>). Latest commit from  ${LATEST_AUTHOR}." , rawMessage: true
    //   }
    //   post {
    //     failure {
    //       rocketSend channel: 'jenkinsohsome', emoji: ':disappointed:', message: "Reporting of *${REPO_NAME}*-build nr. ${env.BUILD_NUMBER} *failed* on Branch - ${env.BRANCH_NAME}  (<${env.BUILD_URL}|Open Build in Jenkins>). Latest commit from  ${LATEST_AUTHOR}." , rawMessage: true
    //     }
    //   }
    // }
  }
}
