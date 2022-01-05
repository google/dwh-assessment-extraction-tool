#!/bin/bash

# Fail on any error.
set -e
set +x

termInstance() {
  gcloud compute instances delete teradata-kokoro --zone us-central1-a --project="${GCP_PROJECT}" --quiet
}

#delete instance after error
trap 'termInstance' ERR

ls "${KOKORO_KEYSTORE_DIR}"
#Variables
TD_PSW=$(<"${KOKORO_KEYSTORE_DIR}"/76474_teradata-12232021)
GCP_PROJECT=$(gcloud config get-value project)
GCP_PROJECT_ID=$(gcloud projects describe "${GCP_PROJECT}" --format 'value(projectNumber)')
GCP_SERVICE_ACCOUNT="${GCP_PROJECT_ID}"-compute@developer.gserviceaccount.com
GCP_IMAGE=projects/"${GCP_PROJECT}"/global/images/teradata1610-ubuntu20
GCP_SCOPES="https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,"\
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

sleep 5m

# Code under repo is checked out to ${KOKORO_ARTIFACTS_DIR}/github.
# The final directory name in this path is determined by the scm name specified
# in the job configuration.
export CLASSPATH="${KOKORO_ARTIFACTS_DIR}/piper/google3/third_party/java/jdbc/teradata/terajdbc4.jar"
set -x

cd "${KOKORO_ARTIFACTS_DIR}/github/dwh-assessment-extraction-tool"
mkdir output
rm -r /home/kbuilder/.cache/bazel/_bazel_kbuilder/install/4cfcf40fe067e89c8f5c38e156f8d8ca

#bazel build dist:all
bazel build src/java/com/google/cloud/bigquery/dwhassessment/extractiontool:ExtractionTool_deploy.jar
ls -la

#./dwh-assessment-extraction-tool.sh td-extract --db-address jdbc:teradata://teradata-kokoro/DBS_PORT=1025,DATABASE=dbc --output ./output  --db-user "${TD_PSW}" --db-password "${TD_PSW}"

#cd "${KOKORO_ARTIFACTS_DIR}/github/dwh-assessment-extraction-tool/integ-tests/"
#mvn test -B

#delete instance after tests
termInstance 