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

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

source "${SCRIPT_DIR}/_init.sh"
source "${SCRIPT_DIR}/_utils.sh"

###########################

check_variables_set FLINK_VERSION

###########################

function deploy_staging_jars {
  cd "${SOURCE_DIR}"
  mkdir -p "${RELEASE_DIR}"

  project_version=$(get_pom_version)
  if [[ ${project_version} =~ -SNAPSHOT$ ]]; then
    echo "Jars should not be created for SNAPSHOT versions. Use 'update_branch_version.sh' first."
    exit 1
  fi
  flink_minor_version=$(echo ${FLINK_VERSION} | sed "s/.[0-9]\+$//")
  version=${project_version}-${flink_minor_version}

  echo "Deploying jars v${version} to repository.apache.org"
  echo "To revert this step, login to 'https://repository.apache.org' -> 'Staging repositories' -> Select repository -> 'Drop'"

  clone_dir=$(create_pristine_source "${SOURCE_DIR}" "${RELEASE_DIR}")
  cd "${clone_dir}"
  set_pom_version "${version}"

  options="-Prelease,docs-and-source -DskipTests -DretryFailedDeploymentCount=10"
  ${MVN} clean deploy ${options} -Dflink.version=${FLINK_VERSION}

  cd "${RELEASE_DIR}"
  rm -rf "${clone_dir}"
}

(deploy_staging_jars)
