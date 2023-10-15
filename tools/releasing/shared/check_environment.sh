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

EXIT_CODE=0

function check_program_available {
  if program=$(command -v ${1}); then
    printf "\t%-10s%s\n" "${1}" "using ${program}"
  else
    printf "\t%-10s%s\n" "${1}" "is not available."
    EXIT_CODE=1
  fi
}

echo "Checking program availability:"
check_program_available git
check_program_available tar
check_program_available rsync
check_program_available gpg
check_program_available perl
check_program_available sed
check_program_available svn
check_program_available ${MVN}
check_program_available ${SHASUM}

function check_git_connectivity {
  cd "${SOURCE_DIR}"
  if git ls-remote --exit-code ${REMOTE} &> /dev/null; then
    printf "\tUsing git remote '${REMOTE}'.\n"
  else
    printf "\tGit remote '${REMOTE}' is not available.\n"
    printf "\tRun 'git remote add upstream https://github.com/apache/<repo>' or set a custom remote with the 'REMOTE' env variable.\n"
    exit 1
  fi
}

echo "Checking git remote availability:"
if ! (check_git_connectivity); then
  EXIT_CODE=1
fi

if [ ${EXIT_CODE} == 0 ]; then
  echo "All set! :)"
else
  echo "At least one problem was found!"
fi
exit ${EXIT_CODE}
