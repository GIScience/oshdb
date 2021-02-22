pipeline {
  agent {label 'main'}

  environment {
    REPO_NAME = sh(returnStdout: true, script: 'basename `git remote get-url origin` .git').trim()
    VERSION = sh(returnStdout: true, script: 'mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -Ev "(^\\[|Download\\w+)"').trim()
    LATEST_AUTHOR = sh(returnStdout: true, script: 'git show -s --pretty=%an').trim()
    LATEST_COMMIT_ID = sh(returnStdout: true, script: 'git describe --tags --long  --always').trim()

    MAVEN_TEST_OPTIONS = ' '
    INFER_BRANCH_REGEX = /(^master$)/
    SNAPSHOT_BRANCH_REGEX = /(^master$)/
    // START CUSTOM oshdb
    BENCHMARK_EXAMPLES_BRANCH_REGEX = /(^master$)/
    // END CUSTOM oshdb
    RELEASE_REGEX = /^([0-9]+(\.[0-9]+)*)(-(RC|beta-|alpha-)[0-9]+)?$/
    RELEASE_DEPLOY = false
    SNAPSHOT_DEPLOY = false
  }

  stages {
    stage ('Build and Test') {
      steps {
        script {
          env.MAVEN_HOME = '/usr/share/maven'

          echo REPO_NAME
          echo LATEST_AUTHOR
          echo LATEST_COMMIT_ID

          echo env.BRANCH_NAME
          echo env.BUILD_NUMBER
          echo env.TAG_NAME

          if (!(VERSION ==~ RELEASE_REGEX || VERSION ==~ /.*-SNAPSHOT$/)) {
            echo 'Version:'
            echo VERSION
            error 'The version declaration is invalid. It is neither a release nor a snapshot. Maybe an error occured while fetching the parent pom using maven?'
          }
        }
        script {
          server = Artifactory.server 'HeiGIT Repo'
          rtMaven = Artifactory.newMavenBuild()

          rtMaven.resolver server: server, releaseRepo: 'main', snapshotRepo: 'main'
          rtMaven.deployer server: server, releaseRepo: 'libs-release-local', snapshotRepo: 'libs-snapshot-local'
          rtMaven.deployer.deployArtifacts = false

          withCredentials([string(credentialsId: 'gpg-signing-key-passphrase', variable: 'PASSPHRASE')]) {
            // START CUSTOM oshdb
            // CUSTOM: added withDep profile
            buildInfo = rtMaven.run pom: 'pom.xml', goals: 'clean compile javadoc:jar source:jar install -P sign,git,withDep -Dmaven.repo.local=.m2 $MAVEN_TEST_OPTIONS -Dgpg.passphrase=$PASSPHRASE'
            // END CUSTOM oshdb
          }
        }
      }
      post {
        failure {
          rocketSend channel: 'jenkinsohsome', emoji: ':sob:' , message: "*${REPO_NAME}*-build nr. ${env.BUILD_NUMBER} *failed* on Branch - ${env.BRANCH_NAME}  (<${env.BUILD_URL}|Open Build in Jenkins>). Latest commit from  ${LATEST_AUTHOR}. Review the code!" , rawMessage: true
        }
      }
    }

    stage ('Deploy Snapshot') {
      when {
        expression {
          return env.BRANCH_NAME ==~ SNAPSHOT_BRANCH_REGEX && VERSION ==~ /.*-SNAPSHOT$/
        }
      }
      steps {
        script {
          rtMaven.deployer.deployArtifacts buildInfo
          server.publishBuildInfo buildInfo
          SNAPSHOT_DEPLOY = true
        }
      }
      post {
        failure {
          rocketSend channel: 'jenkinsohsome', emoji: ':disappointed:', message: "Deployment of *${REPO_NAME}*-build nr. ${env.BUILD_NUMBER} *failed* on Branch - ${env.BRANCH_NAME}  (<${env.BUILD_URL}|Open Build in Jenkins>). Latest commit from  ${LATEST_AUTHOR}. Is Artifactory running?" , rawMessage: true
        }
      }
    }

    stage ('Deploy Release') {
      when {
        expression {
          return VERSION ==~ RELEASE_REGEX && env.TAG_NAME ==~ RELEASE_REGEX
        }
      }
      steps {
        script {
          rtMaven.deployer.deployArtifacts buildInfo
          server.publishBuildInfo buildInfo
          RELEASE_DEPLOY = true
        }
        withCredentials([
            file(credentialsId: 'ossrh-settings', variable: 'settingsFile'),
            string(credentialsId: 'gpg-signing-key-passphrase', variable: 'PASSPHRASE')
        ]) {
          // copy of the above build, since "deploy" does rebuild the packages, without withDep profile
          sh 'mvn -s $settingsFile javadoc:jar source:jar deploy -P sign,git,deploy-central -Dmaven.repo.local=.m2 -Dgpg.passphrase=$PASSPHRASE -DskipTests=true'
        }
      }
      post {
        failure {
          rocketSend channel: 'jenkinsohsome', emoji: ':disappointed:', message: "Deployment of *${REPO_NAME}*-build nr. ${env.BUILD_NUMBER} *failed* on Branch - ${env.BRANCH_NAME}  (<${env.BUILD_URL}|Open Build in Jenkins>). Latest commit from  ${LATEST_AUTHOR}. Is Artifactory running?" , rawMessage: true
        }
      }
    }

    // START CUSTOM oshdb
    stage ('Trigger Benchmark and build Examples') {
      when {
        expression {
          return env.BRANCH_NAME ==~ BENCHMARK_EXAMPLES_BRANCH_REGEX
        }
      }
      steps {
        build job: 'oshdb-benchmark/master', quietPeriod: 360, wait: false
        build job: 'oshdb-examples/master', quietPeriod: 360, wait: false
      }
      post {
        failure {
          rocketSend channel: 'jenkinsohsome', emoji: ':disappointed:', message: "Triggering of Benchmarks or Examples for ${REPO_NAME}-build nr. ${env.BUILD_NUMBER} *failed* on Branch - ${env.BRANCH_NAME}  (<${env.BUILD_URL}|Open Build in Jenkins>). Does the benchmark job still exist?" , rawMessage: true
        }
      }
    }
    // END CUSTOM oshdb

    stage ('Publish Javadoc') {
      when {
        anyOf {
          equals expected: true, actual: RELEASE_DEPLOY
          equals expected: true, actual: SNAPSHOT_DEPLOY
        }
      }
      steps {
        script {
          // load dependencies to artifactory
          rtMaven.run pom: 'pom.xml', goals: 'org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version -Dmaven.repo.local=.m2'

          javadc_dir = "/srv/javadoc/java/" + REPO_NAME + "/" + VERSION + "/"
          echo javadc_dir

          rtMaven.run pom: 'pom.xml', goals: 'clean javadoc:javadoc -Dadditionalparam=-Xdoclint:none -Dmaven.repo.local=.m2'
          sh "echo ${javadc_dir}"
          // make sure jenkins uses bash not dash!
          sh "mkdir -p ${javadc_dir} && rm -Rf ${javadc_dir}* && find . -path '*/target/site/apidocs' -exec cp -R --parents {} ${javadc_dir} \\; && find ${javadc_dir} -path '*/target/site/apidocs' | while read line; do echo \$line; neu=\${line/target\\/site\\/apidocs/} ;  mv \$line/* \$neu ; done && find ${javadc_dir} -type d -empty -delete"
        }

        // START CUSTOM oshdb
        script {
          javadc_dir = javadc_dir + "aggregated/"
          rtMaven.run pom: 'pom.xml', goals: 'clean javadoc:aggregate -Dadditionalparam=-Xdoclint:none -Dmaven.repo.local=.m2'
          sh "mkdir -p ${javadc_dir} && rm -Rf ${javadc_dir}* && find . -path './target/site/apidocs' -exec cp -R --parents {} ${javadc_dir} \\; && find ${javadc_dir} -path '*/target/site/apidocs' | while read line; do echo \$line; neu=\${line/target\\/site\\/apidocs/} ;  mv \$line/* \$neu ; done && find ${javadc_dir} -type d -empty -delete"
        }
        // END CUSTOM oshdb
      }
      post {
        failure {
          rocketSend channel: 'jenkinsohsome', emoji: ':disappointed:', message: "Deployment of javadoc *${REPO_NAME}*-build nr. ${env.BUILD_NUMBER} *failed* on Branch - ${env.BRANCH_NAME}  (<${env.BUILD_URL}|Open Build in Jenkins>). Latest commit from  ${LATEST_AUTHOR}." , rawMessage: true
        }
      }
    }

    stage ('Reports and Statistics') {
      steps {
        script {
          report_basedir = "/srv/reports/${REPO_NAME}/${VERSION}_${env.BRANCH_NAME}/${env.BUILD_NUMBER}_${LATEST_COMMIT_ID}"

          // jacoco
          report_dir = report_basedir + "/jacoco/"

          rtMaven.run pom: 'pom.xml', goals: 'clean verify -Pjacoco -Dmaven.repo.local=.m2 $MAVEN_TEST_OPTIONS'
          jacoco(
              execPattern      : '**/target/jacoco.exec',
              classPattern     : '**/target/classes',
              sourcePattern    : '**/src/main/java',
              inclusionPattern : '/org/heigit/**'
          )
          sh "mkdir -p ${report_dir} && rm -Rf ${report_dir}* && find . -path '*/target/site/jacoco' -exec cp -R --parents {} ${report_dir} \\; && find ${report_dir} -path '*/target/site/jacoco' | while read line; do echo \$line; neu=\${line/target\\/site\\/jacoco/} ;  mv \$line/* \$neu ; done && find ${report_dir} -type d -empty -delete"

          // infer
          if (env.BRANCH_NAME ==~ INFER_BRANCH_REGEX) {
            report_dir = report_basedir + "/infer/"
            sh "mvn clean"
            sh "infer run --pmd-xml -r -- mvn compile"
            sh "mkdir -p ${report_dir} && rm -Rf ${report_dir}* && cp -R ./infer-out/* ${report_dir}"
          }

          // warnings plugin
          rtMaven.run pom: 'pom.xml', goals: '--batch-mode -V -e compile checkstyle:checkstyle pmd:pmd pmd:cpd com.github.spotbugs:spotbugs-maven-plugin:3.1.7:spotbugs -Dmaven.repo.local=.m2'

          recordIssues enabledForFailure: true, tools: [mavenConsole(),  java(), javaDoc()]
          recordIssues enabledForFailure: true, tool: checkStyle()
          recordIssues enabledForFailure: true, tool: spotBugs()
          recordIssues enabledForFailure: true, tool: cpd(pattern: '**/target/cpd.xml')
          recordIssues enabledForFailure: true, tool: pmdParser(pattern: '**/target/pmd.xml')
          recordIssues enabledForFailure: true, tool: pmdParser(pattern: '**/infer-out/report.xml', id: 'infer')
        }
      }
      post {
        failure {
          rocketSend channel: 'jenkinsohsome', emoji: ':disappointed:', message: "Reporting of *${REPO_NAME}*-build nr. ${env.BUILD_NUMBER} *failed* on Branch - ${env.BRANCH_NAME}  (<${env.BUILD_URL}|Open Build in Jenkins>). Latest commit from  ${LATEST_AUTHOR}." , rawMessage: true
        }
      }
    }

    stage ('Check Dependencies') {
      when {
        expression {
          if (currentBuild.number > 1) {
            month_pre = new Date(currentBuild.previousBuild.rawBuild.getStartTimeInMillis())[Calendar.MONTH]
            echo month_pre.toString()
            month_now = new Date(currentBuild.rawBuild.getStartTimeInMillis())[Calendar.MONTH]
            echo month_now.toString()
            return month_pre != month_now
          }
          return false
        }
      }
      steps {
        script {
          update_notify = sh(returnStdout: true, script: 'mvn versions:display-dependency-updates | grep -Pzo "(?s)The following dependencies.*\\n.* \\n"').trim()
          echo update_notify
        }
        rocketSend channel: 'jenkinsohsome', emoji: ':wave:' , message: "Check your dependencies in *${REPO_NAME}*. You might have updates: ${update_notify}" , rawMessage: true
      }
      post {
        failure {
          rocketSend channel: 'jenkinsohsome', emoji: ':disappointed:' , message: "Checking for updates in *${REPO_NAME}*-build nr. ${env.BUILD_NUMBER} *failed* on Branch - ${env.BRANCH_NAME}  (<${env.BUILD_URL}|Open Build in Jenkins>). Latest commit from  ${LATEST_AUTHOR}." , rawMessage: true
        }
      }
    }

    stage ('Encourage') {
      when {
        expression {
          if (currentBuild.number > 1) {
            date_pre = new Date(currentBuild.previousBuild.rawBuild.getStartTimeInMillis()).clearTime()
            echo date_pre.format( 'yyyyMMdd' )
            date_now = new Date(currentBuild.rawBuild.getStartTimeInMillis()).clearTime()
            echo date_now.format( 'yyyyMMdd' )
            return date_pre.numberAwareCompareTo(date_now)<0
          }
          return false
        }
      }
      steps {
        rocketSend channel: 'jenkinsohsome', emoji: ':wink:', message: "Hey, this is just your daily notice that Jenkins is still working for you on *${REPO_NAME}* Branch ${env.BRANCH_NAME}! Happy and for free! Keep it up!" , rawMessage: true
      }
      post {
        failure {
          rocketSend channel: 'jenkinsohsome', emoji: ':disappointed:', message: "Reporting of *${REPO_NAME}*-build nr. ${env.BUILD_NUMBER} *failed* on Branch - ${env.BRANCH_NAME}  (<${env.BUILD_URL}|Open Build in Jenkins>). Latest commit from  ${LATEST_AUTHOR}." , rawMessage: true
        }
      }
    }

    stage ('Report Status Change') {
      when {
        expression {
          return ((currentBuild.number > 1) && (currentBuild.getPreviousBuild().result == 'FAILURE'))
        }
      }
      steps {
        rocketSend channel: 'jenkinsohsome', emoji: ':sunglasses:', message: "We had some problems, but we are BACK TO NORMAL! Nice debugging: *${REPO_NAME}*-build-nr. ${env.BUILD_NUMBER} *succeeded* on Branch - ${env.BRANCH_NAME}  (<${env.BUILD_URL}|Open Build in Jenkins>). Latest commit from  ${LATEST_AUTHOR}." , rawMessage: true
      }
      post {
        failure {
          rocketSend channel: 'jenkinsohsome', emoji: ':disappointed:', message: "Reporting of *${REPO_NAME}*-build nr. ${env.BUILD_NUMBER} *failed* on Branch - ${env.BRANCH_NAME}  (<${env.BUILD_URL}|Open Build in Jenkins>). Latest commit from  ${LATEST_AUTHOR}." , rawMessage: true
        }
      }
    }
  }
}
