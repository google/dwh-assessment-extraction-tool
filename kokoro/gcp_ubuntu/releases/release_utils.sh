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

err() {
  printf '%s\n' "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
  exit 1
}

log() {
  printf '%s\n' "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*"
}

#######################################
# makes HTTP requests and returns a status code
# Globals:
#   None
# Arguments:
#   A non optional HTTP header, a url
# Outputs:
#   A http status code
#######################################
http_get_error_code() {
  http_header="$1"
  http_url="$2"

  http_response="$(curl -s -w "%{http_code}" -H ${http_header} ${http_url})"
  echo $(tail -n1 <<< "${http_response}")
}

#######################################
# makes HTTP post requests, expects given status code
# Globals:
#   None
# Arguments:
#   Expected status code
#   A non optional HTTP header
#   A url
#   Content to POST
# Outputs:
#   A http status code
#######################################
http_post_check_status() {
  expected_http_status="$1"
  http_header="$2"
  http_url="$3"
  post_data="$4"
  
  http_response="$(curl -s -w "%{http_code}" -X POST -H ${http_header} ${http_url} -d "${post_data}" )"

  http_response_code="$(tail -n1 <<< "${http_response}")"  # get the last line
  http_response_content="$(sed '$ d' <<< "${http_response}")" # get all but the last line which contains the status code
  
  if [ "${http_response_code}" !=  "${expected_http_status}" ]; then
    err "ERROR! Http request failed with code ${http_response_code}," \
      "response content is ${http_response_content}"   # write error message to stderr
  fi
  echo "${http_response_content:3}" 
}

#######################################
# Increments the last digit of the version tag
# Globals:
#   None
# Arguments:
#   A tag name in format vX.Y.Z
# Outputs:
#   A new tag name
#######################################
increment_tag_version() {
  local IFS="."
  local arr
  read -ra arr <<< "$1"
  arr[2]="$((arr[2] + 1))"
  echo "${arr[*]}"
}
