#!/bin/bash

# Fail on any error.
set -e

#Variables
GCP_PROJECT=$(gcloud config get-value project)
GCP_PROJECT_ID=$(gcloud config get-value project)
GCP_SERVICE_ACCOUNT=$("${GCP_PROJECT_ID}"-compute@developer.gserviceaccount.com)
GCP_IMAGE=$(projects/"${GCP_PROJECT_ID}"/global/images/teradata1610-ubuntu20)
GCP_SCOPES='https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,'\
            'https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,'   \
            'https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append' 

# Display commands being run.
#set -x

# Display commands being run.
# WARNING: please only enable 'set -x' if necessary for debugging, and be very
#  careful if you handle credentials (e.g. from Keystore) with 'set -x':
#  statements like "export VAR=$(cat /tmp/keystore/credentials)" will result in
#  the credentials being printed in build logs.
#  Additionally, recursive invocation with credentials as command-line
#  parameters, will print the full command, with credentials, in the build logs.
# set -x

#Create Kokoro Teradata instance
gcloud compute instances create teradata-kokoro --project=${GCP_PROJECT_ID} --zone=us-central1-a --machine-type=n2-standard-2 \
        --network-interface=subnet=default,no-address --maintenance-policy=MIGRATE --service-account=${GCP_PROJECT_ID}        \
        --scopes=${GCP_SCOPES} --create-disk=auto-delete=yes,boot=yes,device-name=teradata-kokoro,image=${GCP_IMAGE},mode=rw,size=300,\
        type=projects/${GCP_PROJECT}/zones/us-central1-a/diskTypes/pd-balanced --reservation-affinity=any

# Code under repo is checked out to ${KOKORO_ARTIFACTS_DIR}/github.
# The final directory name in this path is determined by the scm name specified
# in the job configuration.
cd "${KOKORO_ARTIFACTS_DIR}/github/dwh-assessment-extraction-tool/integ-tests/"
mvn test