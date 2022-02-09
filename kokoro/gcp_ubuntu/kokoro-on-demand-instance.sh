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

termInstance() {
  gcloud components update
  gcloud compute instances delete "${TD_HOST}" --zone us-central1-a --project="${GCP_PROJECT}" --quiet
}

createInstance() {
  gcloud components update
  gcloud compute instances create "${TD_HOST}" \
      --project="${GCP_PROJECT}" \
      --zone=us-central1-a \
      --machine-type=n2-standard-2 \
      --network-interface=subnet=default,no-address \
      --maintenance-policy=MIGRATE \
      --service-account="${GCP_SERVICE_ACCOUNT}" \
      --scopes="${GCP_SCOPES}" \
      --create-disk=auto-delete=yes,boot=yes,device-name="${TD_HOST}",image="${GCP_IMAGE}",mode=rw,size=300,type=projects/"${GCP_PROJECT}"/zones/us-central1-a/diskTypes/pd-balanced \
      --reservation-affinity=any
}

#Variables
export TD_HOST="teradata-${KOKORO_BUILD_INITIATOR}"
export GCP_PROJECT="$(gcloud config get-value project)"
export GCP_PROJECT_ID="$(gcloud projects describe ${GCP_PROJECT} --format 'value(projectNumber)')"
export GCP_SERVICE_ACCOUNT="${GCP_PROJECT_ID}-compute@developer.gserviceaccount.com"
export GCP_IMAGE="projects/${GCP_PROJECT}/global/images/teradata1610-ubuntu20"
export GCP_SCOPES="https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,"\
"https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,"\
"https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append"

if [ "${INSTANCE_OPERATION}" = "start" ]; then
  createInstance
elif [ "${INSTANCE_OPERATION}" = "stop" ]; then
  termInstance
else
  echo "Value for INSTANCE_OPERATION not correct. Should be start/stop"
  exit 1
fi
