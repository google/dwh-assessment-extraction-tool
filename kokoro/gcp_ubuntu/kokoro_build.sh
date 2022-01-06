#!/bin/bash

# Code under repo is checked out to ${KOKORO_ARTIFACTS_DIR}/github.
# The final directory name in this path is determined by the scm name specified
# in the job configuration.

# Fail on any error.
set -e

termInstance() {
  gcloud compute instances delete teradata-kokoro --zone us-central1-a --project="${GCP_PROJECT}" --quiet
}

#delete instance after error
trap 'termInstance' ERR

ls "${KOKORO_KEYSTORE_DIR}"
#Variables
export TD_PSW=$(<"${KOKORO_KEYSTORE_DIR}"/76474_teradata-12232021)
export TD_USR="dbc"
export TD_DB="dbc"
export TD_HOST="teradata-kokoro"
export EXPORT_PATH="${KOKORO_ARTIFACTS_DIR}/github/dwh-assessment-extraction-tool/bazel-bin/dist/dwh-assessment-extraction-tool/output"
export JDBC_PATH="${KOKORO_ARTIFACTS_DIR}/piper/google3/third_party/java/jdbc/teradata/terajdbc4.jar"
export CLASSPATH="${JDBC_PATH}"
export GCP_PROJECT=$(gcloud config get-value project)
export GCP_PROJECT_ID=$(gcloud projects describe "${GCP_PROJECT}" --format 'value(projectNumber)')
export GCP_SERVICE_ACCOUNT="${GCP_PROJECT_ID}"-compute@developer.gserviceaccount.com
export GCP_IMAGE=projects/"${GCP_PROJECT}"/global/images/teradata1610-ubuntu20
export GCP_SCOPES="https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,"\
"https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,"\
"https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append"


gcloud components update

# Display commands being run.
# WARNING: please only enable 'set -x' if necessary for debugging, and be very
#  careful if you handle credentials (e.g. from Keystore) with 'set -x':
#  statements like "export VAR=$(cat /tmp/keystore/credentials)" will result in
#  the credentials being printed in build logs.
#  Additionally, recursive invocation with credentials as command-line
#  parameters, will print the full command, with credentials, in the build logs.
# set -x

#Create Kokoro Teradata instance
gcloud compute instances create teradata-kokoro --project="${GCP_PROJECT}" --zone=us-central1-a --machine-type=n2-standard-2 \
--network-interface=subnet=default,no-address --maintenance-policy=MIGRATE --service-account="${GCP_SERVICE_ACCOUNT}" \
--scopes="${GCP_SCOPES}" --create-disk=auto-delete=yes,boot=yes,device-name=teradata-kokoro,image="${GCP_IMAGE}",mode=rw,size=300,type=projects/"${GCP_PROJECT}"/zones/us-central1-a/diskTypes/pd-balanced \
--reservation-affinity=any

#Time to allow TD to recover
sleep 5m

#Bugfix for bazel 
use_bazel.sh 4.1.0
command -v bazel
bazel version

cd "${KOKORO_ARTIFACTS_DIR}/github/dwh-assessment-extraction-tool"

rm -r /home/kbuilder/.cache/bazel/_bazel_kbuilder/install/4cfcf40fe067e89c8f5c38e156f8d8ca

#Build extraction tool
bazel build dist:all

cd "${KOKORO_ARTIFACTS_DIR}/github/dwh-assessment-extraction-tool/bazel-bin/dist/"
unzip ./dwh-assessment-extraction-tool.zip

cd "${KOKORO_ARTIFACTS_DIR}/github/dwh-assessment-extraction-tool/bazel-bin/dist/dwh-assessment-extraction-tool"

mkdir output

#Extract data from TD instance
set +x
./dwh-assessment-extraction-tool td-extract --db-address jdbc:teradata://"${TD_HOST}"/DBS_PORT=1025,DATABASE="${TD_DB}" --output "${EXPORT_PATH}"  --db-user "${TD_USR}" --db-password "${TD_PSW}"


#cd "${KOKORO_ARTIFACTS_DIR}/github/dwh-assessment-extraction-tool/integ-tests/"
#mvn test -B

#delete instance after tests
termInstance