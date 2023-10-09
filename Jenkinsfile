pipeline {
    agent { label 'worker' }
    options {
        timeout(time: 30, unit: 'MINUTES')
    }
    tools {
        maven 'Maven 3'
    }

    environment {
        // this regex determines which branch is deployed as a snapshot
        SNAPSHOT_BRANCH_REGEX = /(^master$)/
        BENCHMARK_BRANCH_REGEX = /(^master$)/
        RELEASE_REGEX = /^([0-9]+(\.[0-9]+)*)(-(RC|beta-|alpha-)[0-9]+)?$/
        RELEASE_DEPLOY = false
        SNAPSHOT_DEPLOY = false
    }

    stages {
        stage('Build and Test') {
            steps {
                // setting up a few basic env variables like REPO_NAME and LATEST_AUTHOR
                setup_basic_env()

                mavenbuild('clean compile javadoc:jar source:jar verify -P jacoco,sign,git')
            }
            post {
                failure {
                    rocket_buildfail()
                    rocket_testfail()
                }
            }
        }

        stage('Reports and Statistics') {
            steps {
                reports_sonar_jacoco()
            }
        }

            stage('Deploy Snapshot') {
            when {
                expression {
                    return env.BRANCH_NAME ==~ SNAPSHOT_BRANCH_REGEX && VERSION ==~ /.*-SNAPSHOT$/
                }
            }
            steps {
                deploy_snapshot('clean compile javadoc:jar source:jar deploy -P sign,git,withDep -DskipTests=true')
            }
            post {
                failure {
                    rocket_snapshotdeployfail()
                }
            }
            }

        stage('Deploy Release') {
            when {
                expression {
                    return VERSION ==~ RELEASE_REGEX && env.TAG_NAME ==~ RELEASE_REGEX
                }
            }
            steps {
                deploy_release('clean compile javadoc:jar source:jar deploy -P sign,git,withDep -DskipTests=true')

                withCredentials([
            file(credentialsId: 'ossrh-settings', variable: 'settingsFile'),
            string(credentialsId: 'gpg-signing-key-passphrase', variable: 'PASSPHRASE')
        ]) {
                    sh 'mvn --batch-mode --update-snapshots clean compile -s $settingsFile javadoc:jar source:jar deploy -P sign,git,deploy-central -Dmaven.repo.local=.m2  -Dgpg.passphrase=$PASSPHRASE -DskipTests=true'
        }
            }
            post {
                failure {
                    rocket_releasedeployfail()
                }
            }
        }

        // START CUSTOM oshdb
        stage('Trigger Benchmarks') {
            when {
                expression {
                    return env.BRANCH_NAME ==~ BENCHMARK_BRANCH_REGEX
                }
            }
            steps {
                build job: 'oshdb-benchmark/master', quietPeriod: 360, wait: false
            }
            post {
                failure {
                    rocket_basicsend("Triggering of Benchmarks for ${REPO_NAME}-build nr. ${env.BUILD_NUMBER} *failed* on Branch - ${env.BRANCH_NAME}  (<${env.BUILD_URL}|Open Build in Jenkins>). Does the benchmark job still exist?")
                }
            }
        }

        stage('Build Examples') {
            when {
                anyOf {
                    equals expected: true, actual: RELEASE_DEPLOY
                    equals expected: true, actual: SNAPSHOT_DEPLOY
                }
            }
            steps {
                script {
                    if (RELEASE_DEPLOY == true) {
                        build job: 'oshdb-examples/oshdb-stable', quietPeriod: 360, wait: false
          } else {
                        build job: 'oshdb-examples/oshdb-snapshot', quietPeriod: 360, wait: false
                    }
                }
            }
            post {
                failure {
                    rocket_basicsend("Triggering of Examples build for ${REPO_NAME}-build nr. ${env.BUILD_NUMBER} *failed* on Branch - ${env.BRANCH_NAME}  (<${env.BUILD_URL}|Open Build in Jenkins>).")
                }
            }
        }
    // END CUSTOM oshdb

        stage('Check Dependencies') {
            when {
                expression {
                    return ((currentBuild.getStartTimeInMillis() - currentBuild.previousBuild.getStartTimeInMillis()) > 2592000000000) //2592000000000 one month in milliseconds
                }
            }
            steps {
                check_dependencies()
            }
        }

        stage('Wrapping Up') {
            steps {
                encourage()
                status_change()
            }
        }
    }
}