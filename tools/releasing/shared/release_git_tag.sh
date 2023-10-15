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

source ${SCRIPT_DIR}/_init.sh
source ${SCRIPT_DIR}/_utils.sh

###########################

RC_NUM=${RC_NUM:-none}

###########################

function create_release_tag {
  cd "${SOURCE_DIR}"

  version=$(get_pom_version)
  if [[ ${version} =~ -SNAPSHOT$ ]]; then
    echo "Tags should not be created for SNAPSHOT versions. Use 'update_branch_version.sh' first."
    exit 1
  fi

  tag=v${version}
  if [ "$RC_NUM" != "none" ]; then
    tag=${tag}-rc${RC_NUM}
  fi

  git tag -s -m "v${tag}" ${tag}

  git push ${REMOTE} ${tag}
}

(create_release_tag)
