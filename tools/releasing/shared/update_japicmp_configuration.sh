#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source ${SCRIPT_DIR}/_init.sh
source ${SCRIPT_DIR}/_utils.sh

###########################

check_variables_set NEW_VERSION

###########################

# Idealized use-cases:
# Scenario A) New major release X.0.0
#   Premise:
#     There is a main branch a version X.0-SNAPSHOT, with a japimp reference version of (X-1).Y.Z
#   Release flow:
#     - update the main to (X+1).0-SNAPSHOT, but keep the reference version intact since X.0.0 is not released (yet)
#     - create X.0-SNAPSHOT branch, but keep the reference version intact since X.0.0 is not released (yet)
#     - release X.0.0
#     - update the japicmp reference version of both main and X.0-SNAPSHOT to X.0.0
#     - enable stronger compatibility constraints for X.0-SNAPSHOT to ensure compatibility for PublicEvolving
# Scenario A) New minor release X.Y.0
#   Premise:
#     There is a main branch with a version X.Y-SNAPSHOT, with a japicmp reference version of X.(Y-1).0 .
#   Release flow:
#     - update the main branch to X.(Y+1)-SNAPSHOT, but keep the reference version intact since X.Y.0 is not released (yet)
#     - create X.Y-SNAPSHOT branch, but keep the reference version intact since X.Y.0 is not released (yet)
#     - release X.Y.0
#     - update the japicmp reference version of both main and X.Y-SNAPSHOT to X.Y.0
#     - enable stronger compatibility constraints for X.Y-SNAPSHOT to ensure compatibility for PublicEvolving
# Scenario C) New patch release X.Y.Z
#   Premise:
#     There is a snapshot branch with a version X.Y-SNAPSHOT, with a japicmp reference version of X.Y.(Z-1)
#   Release flow:
#     - release X.Y.Z
#     - update the japicmp reference version of X.Y-SNAPSHOT to X.Y.Z

function enable_public_evolving_compatibility_checks() {
  perl -pi -e 's#<!--(<include>\@org.apache.flink.annotation.PublicEvolving</include>)-->#${1}#' pom.xml
  perl -pi -e 's#\t+<exclude>\@org.apache.flink.annotation.PublicEvolving.*\n##' pom.xml
}

function set_japicmp_reference_version() {
  local version=$1

  perl -pi -e 's#(<japicmp.referenceVersion>).*(</japicmp.referenceVersion>)#${1}'${version}'${2}#' pom.xml
}

function clear_exclusions() {
  exclusion_start=$(($(sed -n '/<!-- MARKER: start exclusions/=' pom.xml) + 1))
  exclusion_end=$(($(sed -n '/<!-- MARKER: end exclusions/=' pom.xml) - 1))

  if [[ $exclusion_start -lt $exclusion_end ]]; then
    sed -i "${exclusion_start},${exclusion_end}d" pom.xml
  fi
}

function update_japicmp_configuration() {
  cd "${SOURCE_DIR}"

  current_branch=$(git branch --show-current)

  if [[ ${current_branch} =~ ^main$ ]]; then
    # main branch
    set_japicmp_reference_version ${NEW_VERSION}
    clear_exclusions
  elif [[ ${current_branch} =~ ^v ]]; then
    # snapshot branch
    set_japicmp_reference_version ${NEW_VERSION}
    enable_public_evolving_compatibility_checks
    clear_exclusions
  else
    echo "Script was called from unexpected branch ${current_branch}; should be snapshot/main branch."
    exit 1
  fi
}

(update_japicmp_configuration)