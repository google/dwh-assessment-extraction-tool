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
# A Kokoro build script to build release and update github repo

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

#Variables
export GIT_PSW="$(<${KOKORO_KEYSTORE_DIR}/76474_bqassessment-github-releases-02032022)"
export GIT_RELEASES_USERNAME="edw-assessment-integration-testing-bot"
export KOKORO_BUILD_RELEASE_DIR="${KOKORO_ARTIFACTS_DIR}/github/dwh-assessment-extraction-tool"
export KOKORO_RELEASE_OUTPUT_FILE="${KOKORO_BUILD_RELEASE_DIR}/bazel-bin/dist/dwh-assessment-extraction-tool.zip"
export KOKORO_BUILD_SCRIPT_DIR="${KOKORO_BUILD_RELEASE_DIR}/kokoro/gcp_ubuntu"
export KOKORO_RELEASE_SCRIPT_DIR="${KOKORO_BUILD_RELEASE_DIR}/kokoro/gcp_ubuntu/releases"
export BUILD_SCRIPT="$KOKORO_BUILD_SCRIPT_DIR/kokoro_build.sh"

source ${KOKORO_RELEASE_SCRIPT_DIR}/release_utils.sh

cd ${KOKORO_BUILD_RELEASE_DIR}

log "Current dir "$(pwd)
git checkout main

export LAST_GIT_TAG=$(git tag  \
    | grep -E '^v[0-9]' \
    | sort -V \
    | tail -1 )

# Do we already know what version we want to release?
if [[ -z "${CREATE_TAG}" ]]; then
  if [[ -z "${LAST_GIT_TAG}" ]]; then
    err "No previous git tag found and it was not provided with CREATE_TAG env"
  fi
  VERSION=$(incrementTagVersion ${LAST_GIT_TAG}) 
else
  VERSION="${CREATE_TAG}"
fi



log "Will create new version "${VERSION}

if [ $(git tag -l "$VERSION") ]; then
  err "ERROR! Tag for specified version already exists! ${VERSION}"  # write error message to stderr, exits
fi

code=$(httpGetErrorCode "Accept: application/vnd.github.v3+json" \
  "https://${GIT_RELEASES_USERNAME}:${GIT_PSW}@api.github.com/repos/google/dwh-assessment-extraction-tool/releases/tags/${VERSION}" )
if [ $code != "404" ]; then
  err "ERROR! Release with ${VERSION} tag version name already exists or http failed! Http code is ${code}" 
fi

log "New version name verified"

#Run build and integration tests
sh $BUILD_SCRIPT

cd "${KOKORO_BUILD_RELEASE_DIR}"

# create and register tag for this release
git tag -a ${VERSION} -m ${VERSION}
log "Create new tag"
git push https://${GIT_RELEASES_USERNAME}:${GIT_PSW}@github.com/google/dwh-assessment-extraction-tool.git ${VERSION}

log "Prepare release notes"

output=$(httpPostCheckStatus  "200" "Accept: application/vnd.github.v3+json" \
    "https://${GIT_RELEASES_USERNAME}:${GIT_PSW}@api.github.com/repos/google/dwh-assessment-extraction-tool/releases/generate-notes" \
    '{"tag_name":"'${VERSION}'","previous_tag_name":"'${LAST_GIT_TAG}'"}' )
release_body=$(jq -r '.body'   <<< $output )""

log "Create release"

payload=$(
  jq --null-input \
     --arg tag "$VERSION" \
     --arg name "$VERSION" \
     --arg body "$release_body" \
     '{ tag_name: $tag, name: $name, body: $body, draft: true }'
)

# Create new release as a draft
response=$(httpPostCheckStatus "201" "Accept: application/vnd.github.v3+json" \
  "https://${GIT_RELEASES_USERNAME}:${GIT_PSW}@api.github.com/repos/google/dwh-assessment-extraction-tool/releases" "$payload" )
  #'{"tag_name":"'${VERSION}'","name":"'${VERSION}'","draft":true,"generate_release_notes":'${generate_unscoped_logs}',"body":"'${release_body}'" }' )

log "Append file to release"

# Attach release binary file to the release
release_id=$(jq -r '.id' <<< $response )""
curl \
   --data-binary @$KOKORO_RELEASE_OUTPUT_FILE \
  -H "Content-Type: application/octet-stream" \
  "https://${GIT_RELEASES_USERNAME}:${GIT_PSW}@uploads.github.com/repos/google/dwh-assessment-extraction-tool/releases/${release_id}/assets?name=dwh-assessment-extraction-tool-${VERSION}.zip"

log "Release was published"