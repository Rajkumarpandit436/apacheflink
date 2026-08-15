pipeline {

    agent any

    environment {
        FLINK_JOBMANAGER = "flink-jobmanager"
        FLINK_JAR_PATH = "/opt/flink/usrlib/apacheflink-0.0.1-SNAPSHOT.jar"
        FLINK_REST_URL = "http://localhost:8082"
        // EXACT SAME name rakhein jo Java code ke env.execute("...") me hai
        TARGET_JOB_NAME = "Flink Job"
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
                        error("No application JAR found in target/")
                    }

                    echo "Application JAR: ${env.JAR_FILE}"
                }
            }
        }

        stage('Copy JAR to Flink') {
            steps {
                sh '''
                    docker exec ${FLINK_JOBMANAGER} mkdir -p /opt/flink/usrlib
                    docker cp "${JAR_FILE}" ${FLINK_JOBMANAGER}:${FLINK_JAR_PATH}
                '''
            }
        }

        stage('Stop ALL Previous Flink Instances') {
            steps {
                script {
                    // Sabhi running instances ki IDs extract karein
                    def jobIds = sh(
                        script: """
                            curl -s ${FLINK_REST_URL}/jobs/overview | python3 -c "
import sys, json
data = json.load(sys.stdin)
target_name = '${env.TARGET_JOB_NAME}'
job_ids = []
for job in data.get('jobs', []):
    if job.get('name') == target_name and job.get('state') in ['RUNNING', 'CREATED', 'INITIALIZING', 'RECONCILING', 'DEPLOYING']:
        job_ids.append(job['jid'])
print(' '.join(job_ids))
"
                        """,
                        returnStdout: true
                    ).trim()

                    if (jobIds) {
                        echo "Found running job instance(s): ${jobIds}"

                        // Har running instance ko cancel karein
                        sh """
                            for JID in ${jobIds}; do
                                echo "Canceling Job ID: \$JID"
                                curl -s -X PATCH "${FLINK_REST_URL}/jobs/\$JID?mode=cancel"
                            done
                        """

                        echo "Waiting for all previous instances to terminate..."

                        // Wait until no job is in RUNNING state
                        sh """
                            for i in \$(seq 1 30); do
                                ACTIVE_COUNT=\$(curl -s ${FLINK_REST_URL}/jobs/overview | python3 -c "
import sys, json
data = json.load(sys.stdin)
target_name = '${env.TARGET_JOB_NAME}'
active = [j for j in data.get('jobs', []) if j.get('name') == target_name and j.get('state') in ['RUNNING', 'INITIALIZING', 'RECONCILING', 'DEPLOYING']]
print(len(active))
")
                                echo "Active instances remaining: \$ACTIVE_COUNT"

                                if [ "\$ACTIVE_COUNT" -eq "0" ]; then
                                    echo "All previous instances stopped successfully."
                                    break
                                fi
                                sleep 2
                            done
                        """
                    } else {
                        echo "No active instance of '${env.TARGET_JOB_NAME}' found."
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

        stage('Verify Single Deployment') {
            steps {
                script {
                    sh '''
                        echo "Verifying new Flink job execution..."

                        for i in $(seq 1 30); do
                            RESPONSE=$(curl -s ${FLINK_REST_URL}/jobs/overview)

                            RUNNING_COUNT=$(echo "$RESPONSE" | python3 -c "
import sys, json
data = json.load(sys.stdin)
target_name = '${TARGET_JOB_NAME}'
running = [j for j in data.get('jobs', []) if j.get('name') == target_name and j.get('state') == 'RUNNING']
print(len(running))
")

                            if [ "$RUNNING_COUNT" -eq "1" ]; then
                                echo "SUCCESS: Exactly 1 instance of '${TARGET_JOB_NAME}' is RUNNING."
                                exit 0
                            elif [ "$RUNNING_COUNT" -gt "1" ]; then
                                echo "ERROR: Multiple instances detected ($RUNNING_COUNT)!"
                                exit 1
                            fi

                            sleep 2
                        done

                        echo "ERROR: Flink Job failed to reach RUNNING state within timeout."
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