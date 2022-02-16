#!/bin/bash
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License
#
# A Kokoro build script to run the extraction tool and integration tests

# Code under repo is checked out to ${KOKORO_ARTIFACTS_DIR}/github.
# The final directory name in this path is determined by the scm name specified
# in the job configuration.

# Display commands being run.
# WARNING: please only enable 'set -x' if necessary for debugging, and be very
#  careful if you handle credentials (e.g. from Keystore) with 'set -x':
#  statements like "export VAR=$(cat /tmp/keystore/credentials)" will result in
#  the credentials being printed in build logs.
#  Additionally, recursive invocation with credentials as command-line
#  parameters, will print the full command, with credentials, in the build logs.
# set -x

# Fail on any error.
set -e

#delete instance after error
trap 'termInstance' ERR

termInstance() {
  echo "Error trapped, line $(caller)" >&2
  trap - ERR
  gcloud compute instances delete "${TD_HOST}" --zone us-central1-a --project="${GCP_PROJECT}" --quiet

  #Generate test report
  mvn surefire-report:report-only -B -e
  #Rename Maven-surefire test reports for suitable for Sponge format
  cd "${KOKORO_ARTIFACTS_DIR}/github/dwh-assessment-extraction-tool/integ-tests/target/surefire-reports/"
  for f in *.xml; do
    mv -- "$f" "${f%.xml}_sponge_log.xml"
  done

  for f in *.txt; do
    mv -- "$f" "${f%.txt}_sponge_log.log"
  done

}

#Variables
export KOKORO_PROJECT_NAME="${KOKORO_JOB_NAME##*/}"
export TD_PSW="$(<${KOKORO_KEYSTORE_DIR}/76474_teradata-12232021)"
export TD_USR="dbc"
export TD_DB="dbc"
export TD_HOST="teradata-kokoro-${KOKORO_PROJECT_NAME}"
export EXPORT_PATH="${KOKORO_ARTIFACTS_DIR}/github/dwh-assessment-extraction-tool/bazel-bin/dist/dwh-assessment-extraction-tool/output/"
export JDBC_PATH="${KOKORO_ARTIFACTS_DIR}/piper/google3/third_party/java/jdbc/teradata/terajdbc4.jar"
export CLASSPATH="${JDBC_PATH}"
export GCP_PROJECT="$(gcloud config get-value project)"
export GCP_PROJECT_ID="$(gcloud projects describe ${GCP_PROJECT} --format 'value(projectNumber)')"
export GCP_SERVICE_ACCOUNT="${GCP_PROJECT_ID}-compute@developer.gserviceaccount.com"
export GCP_IMAGE="projects/${GCP_PROJECT}/global/images/teradata1610-ubuntu20"
export GCP_SCOPES="https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,"\
"https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,"\
"https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append"

export START_DATE=$(TZ=America/Los_Angeles date --date="10 years ago" +"%Y-%m-%dT%H:%M:%S.%6N")
export END_DATE=$(TZ=America/Los_Angeles date --date="10 minutes ago" +"%Y-%m-%dT%H:%M:%S.%6N")

gcloud components update

#Create Kokoro Teradata instance
gcloud compute instances create "${TD_HOST}" \
    --project="${GCP_PROJECT}" \
    --zone=us-central1-a \
    --machine-type=n2-standard-4 \
    --network-interface=subnet=default,no-address \
    --maintenance-policy=MIGRATE \
    --service-account="${GCP_SERVICE_ACCOUNT}" \
    --scopes="${GCP_SCOPES}" \
    --create-disk=auto-delete=yes,boot=yes,device-name="${TD_HOST}",image="${GCP_IMAGE}",mode=rw,size=300,type=projects/"${GCP_PROJECT}"/zones/us-central1-a/diskTypes/pd-balanced \
    --reservation-affinity=any

#Bugfix for bazel
use_bazel.sh 4.1.0
command -v bazel
bazel version

cd "${KOKORO_ARTIFACTS_DIR}/github/dwh-assessment-extraction-tool"

#Build extraction tool
bazel build dist:all

cd "${KOKORO_ARTIFACTS_DIR}/github/dwh-assessment-extraction-tool/bazel-bin/dist/"
unzip ./dwh-assessment-extraction-tool.zip

#Time to allow TD to recover
sleep 4m

#Generate Test Data
cd "${KOKORO_ARTIFACTS_DIR}/github/dwh-assessment-extraction-tool/integ-tests"
mvn clean compile exec:java -e -B -Dtest="${FUNC_TESTS}"

cd "${KOKORO_ARTIFACTS_DIR}/github/dwh-assessment-extraction-tool/bazel-bin/dist/dwh-assessment-extraction-tool"

mkdir output

#Extract data from TD instance
set +x
./dwh-assessment-extraction-tool td-extract \
    --db-address jdbc:teradata://"${TD_HOST}"/DBS_PORT=1025,DATABASE="${TD_DB}" \
    --output "${EXPORT_PATH}" \
    --db-user "${TD_USR}" \
    --db-password "${TD_PSW}" \
    --qrylog-timerange-start "${START_DATE}" \
    --qrylog-timerange-end "${END_DATE}"

#How many exported avro files
exported_avro=$(ls "${EXPORT_PATH}" | wc -l)

if ((exported_avro != 16)); then
  printf '%s\n' "ERROR! Not all avro files have been exported! ${exported_avro} exported" >&2  # write error message to stderr
  termInstance
  exit 1
else
  printf '%s\n' "${exported_avro} avro files successfully exported."
fi

#Execute integration tests
cd "${KOKORO_ARTIFACTS_DIR}/github/dwh-assessment-extraction-tool/integ-tests"
mvn clean test -B -e

#delete instance after tests
termInstance