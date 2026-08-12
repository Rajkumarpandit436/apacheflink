pipeline {

    agent any

    environment {
        FLINK_JOBMANAGER = "flink-jobmanager"
        FLINK_JAR_PATH = "/opt/flink/usrlib/apacheflink-0.0.1-SNAPSHOT.jar"
        FLINK_REST_URL = "http://localhost:8082"
    }

    stages {

        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Build') {
            steps {
                sh '''
                    chmod +x mvnw
                    ./mvnw clean package -DskipTests
                '''
            }
        }

        stage('Find JAR') {
            steps {
                script {
                    env.JAR_FILE = sh(
                        script: "find target -maxdepth 1 -name '*.jar' ! -name '*.original' -print -quit",
                        returnStdout: true
                    ).trim()

                    if (!env.JAR_FILE) {
                        error("No application JAR found")
                    }

                    echo "Application JAR: ${env.JAR_FILE}"
                }
            }
        }

        stage('Copy JAR to Flink') {
            steps {
                sh '''
                    docker exec ${FLINK_JOBMANAGER} \
                        mkdir -p /opt/flink/usrlib

                    docker cp "${JAR_FILE}" \
                        ${FLINK_JOBMANAGER}:${FLINK_JAR_PATH}
                '''
            }
        }

        stage('Stop Previous Flink Job') {
            steps {
                script {

                    def jobId = sh(
                        script: '''
                            curl -s ${FLINK_REST_URL}/jobs/overview |
                            python3 -c "
import sys,json
data=json.load(sys.stdin)
jobs=data.get('jobs',[])
for job in jobs:
    if job.get('name') == 'Flink Job' and job.get('state') in ['RUNNING','CREATED','INITIALIZING','RECONCILING','DEPLOYING']:
        print(job['jid'])
        break
"
                        ''',
                        returnStdout: true
                    ).trim()

                    if (jobId) {
                        echo "Previous Flink Job found: ${jobId}"

                        sh """
                            curl -s -X PATCH \
                                ${FLINK_REST_URL}/jobs/${jobId}?mode=cancel
                        """

                        echo "Waiting for previous job to stop..."

                        sh """
                            for i in \$(seq 1 30); do

                                STATE=\$(curl -s \
                                    ${FLINK_REST_URL}/jobs/${jobId} |
                                    python3 -c "import sys,json; print(json.load(sys.stdin).get('state',''))")

                                echo "Previous job state: \$STATE"

                                if [ "\$STATE" = "CANCELED" ] || \
                                   [ "\$STATE" = "FAILED" ] || \
                                   [ "\$STATE" = "FINISHED" ]; then
                                    break
                                fi

                                sleep 2
                            done
                        """
                    } else {
                        echo "No previous Flink Job found."
                    }
                }
            }
        }

        stage('Submit Flink Job') {
            steps {
                sh '''
                    docker exec ${FLINK_JOBMANAGER} \
                        flink run -d ${FLINK_JAR_PATH}
                '''
            }
        }

        stage('Verify Deployment') {
            steps {
                script {

                    sh '''
                        echo "Waiting for Flink job..."

                        for i in $(seq 1 30); do

                            RESPONSE=$(curl -s ${FLINK_REST_URL}/jobs/overview)

                            echo "$RESPONSE"

                            RUNNING=$(echo "$RESPONSE" | python3 -c "
import sys,json
data=json.load(sys.stdin)
print(any(
    job.get('name') == 'Flink Job' and
    job.get('state') == 'RUNNING'
    for job in data.get('jobs',[])
))
")

                            if [ "$RUNNING" = "True" ]; then
                                echo "Flink Job is RUNNING"
                                exit 0
                            fi

                            sleep 2
                        done

                        echo "Flink Job did not reach RUNNING state"
                        exit 1
                    '''
                }
            }
        }
    }

    post {

        success {
            echo 'Flink deployment successful!'
        }

        failure {
            echo 'Flink deployment failed!'
        }
    }
}