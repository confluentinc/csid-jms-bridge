#!/bin/bash -e
REPO_ROOT=$(git rev-parse --show-toplevel)

function usage {
        echo "Usage: $(basename $0) [-chlX] [-t test-scenario] [-p json-prop-file] [-k private-gcp-key] [-u gcp-user] [-e execute-playbook]"
        echo "    -h )"
        echo "        Show usage"
        echo ""
        echo "    -X )"
        echo "        Tear down and destroy any remaining infrastructure."
        echo ""
        echo "    -c )"
        echo "        Clean, rebuild and reinstall all binaries."
        echo ""
        echo "    -t test-scenario )"
        echo "        Execute the given test scenario."
        echo ""
        echo "    -l )"
        echo "        List available test scenarios and playbooks."
        echo ""
        echo "    -p json-prop-file )"
        echo "        Use the given json file to populate variables for the test."
        echo ""
        echo "    -k private-gcp-key )"
        echo "        The ssh key used by GCP to allow access to GCE instances started by terraform."
        echo "        Typically found at '~/.ssh/google_compute_engine'"
        echo ""
        echo "    -u gcp-user )"
        echo "        The GCP user login name associated to GCE instances started by terraform."
        echo "        Typically this is your email with all special characters replaced with underscores."
        echo "          E.g. john.doe@company.com => john_doe_company_com"
        echo ""
        echo "    -e execute-playbook )"
        echo "        The specific playbook to execute, jms-bridge, jms-bridge-perf or controller."
        echo ""
}

function errout {
  echo "$1" >&2
}

function list_scenarios {
    errout "Available scenarios:"
    pushd "${REPO_ROOT}/jms-bridge-perf/scenarios" &>/dev/null || exit 1
    find . -name "*.json" -type f \
      | sed -e 's#^\./\(.*\)\.json$#  \1#' >&2

    popd &>/dev/null || exit 1
}

function list_playbooks {
    errout "Available playbooks:"
    pushd "${REPO_ROOT}/jms-bridge-perf/ansible" &>/dev/null || exit 1
    find . -name "*-playbook.yml" -type f \
      | sed -e 's#^\./\(.*\)-playbook\.yml$#  \1#' >&2

    popd &>/dev/null || exit 1
}

function abs_path {
      echo "$(cd "$(dirname "$1")"; pwd)/$(basename "$1")"
}

function scenario {
  local s_name=${1:-''}
  local s_json="${REPO_ROOT}/jms-bridge-perf/scenarios/${s_name}.json"

  if [ ! -f "$s_json" ]; then
    errout "Scenario '${s_name}' not found!"
    list_scenarios
    exit 1
  fi

  echo "$s_json"
}


function do_terraform {
  local destroy=${1:-0}

  # Terraform
  pushd "${REPO_ROOT}/jms-bridge-perf/tf" &>/dev/null || exit 1

  if [ "$destroy" -eq 1 ]; then
    errout "Executing terraform destroy"
    terraform destroy >&2
  else
    terraform plan &> /dev/null
    if [ ! $? -eq 2 ]; then
      errout "Executing terraform apply"
      terraform apply >&2
    else
      errout "Terraform is up to date"
    fi
  fi
  popd &>/dev/null || exit 1
}

# list of arguments expected in the input
optstring=":hclXt:p:u:k:e:"
_clean=0
_scenario=''
_props=''
_gcpkey="$HOME/.ssh/google_compute_engine"
_gcpuser=''
_playbook='site.yml'

while getopts ${optstring} arg; do
  case ${arg} in
    h)
      usage
      exit 0
      ;;
    X)
      errout "Calling terraform to destroy environment"
      do_terraform 1
      exit 0
      ;;
    c)
      _clean=1
      ;;
    l)
      list_scenarios
      errout ''
      list_playbooks
      exit 0
      ;;
    t)
      _scenario=$(scenario ${OPTARG})
      ;;
    p)
      _props=$(abs_path "${OPTARG}")
      if [ ! -f "$_props" ]; then
        errout "Invalid properties path: ${OPTARG}"
        exit 1
      fi
      ;;
    u)
      _gcpuser="${OPTARG}"
      ;;
    k)
      _gcpkey=$(abs_path "${OPTARG}")
      if [ ! -f "$_gcpkey" ]; then
        errout "Invalid private gcp key path: ${OPTARG}"
        exit 1
      fi
       ;;
    e)
      _playbook="${OPTARG}-playbook.yml"
      if [ ! -f "${REPO_ROOT}/jms-bridge-perf/ansible/${_playbook}" ]; then
        errout "Invalid playbook given: ${OPTARG}"
        list_playbooks
        exit 1
      fi
     ;;
    :)
      errout "$0: Must supply an argument to -$OPTARG."
      exit 1
      ;;
    ?)
      errout "Invalid option: -${OPTARG}."
      usage
      exit 2
      ;;
  esac
done

_errs=0
if ! command -v ansible &>/dev/null; then _errs=1; errout "ERROR: ansible is required"; fi
if ! command -v terraform &>/dev/null; then _errs=1; errout "ERROR: terraform is required"; fi
if ! command -v gcloud &>/dev/null; then _errs=1; errout "ERROR: gcloud is required"; fi
if [ "x$_scenario" == 'x' ]; then _errs=1; errout "ERROR: -t, scenario is required"; fi
if [ "x$_gcpuser" == 'x' ]; then _errs=1; errout "ERROR: -u, GCP user is required"; fi
if [ $_errs -eq 1 ]; then usage; exit 1; fi

# Maven
errout "Collecting version information from Maven"
pushd "${REPO_ROOT}" &>/dev/null || exit 1
_version=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
_jms_archive="${REPO_ROOT}/jms-bridge-server/target/jms-bridge-server-${_version}-package.zip"
_perf_archive="${REPO_ROOT}/jms-bridge-perf/target/jms-bridge-perf-${_version}-package.tar.gz"
errout "    version: ${_version}"

if [ ! -f "$_jms_archive" ] || [ ! -f "$_perf_archive" ] || [ $_clean -eq 1 ]
then
  errout "Executing maven build: [mvn clean package -DskipTests -q"
  mvn clean package -DskipTests -q >&2
fi
popd &>/dev/null || exit 1

# Terraform
errout "Executing terraform"
do_terraform

# Ansible
pushd "${REPO_ROOT}/jms-bridge-perf/ansible" &>/dev/null || exit 1
_tags='untagged'
if [ $_clean -eq 1 ]; then
  _tags='all,clean'
fi
errout "Executing ansible"
ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook \
  -u "${_gcpuser}" \
  --private-key "${_gcpkey}" \
  -i inventory.gcp.yml \
  --extra-vars "@${_scenario}" \
  --extra-vars "@${_props}" \
  -e "jms_bridge_archive=${_jms_archive}" \
  -e "jms_bridge_version=${_version}" \
  -e "perf_archive=${_perf_archive}" \
  --tags "$_tags" \
  "$_playbook" >&2

errout "To see results execute:"
errout "  gcloud compute ssh bridge-perf-controller -- -L 3000:localhost:3000 -L 8686:localhost:8686"
errout ""
errout "Then open http://localhost:3000/d/9M1xLswMx/jms-bridge-performance?orgId=1&from=now-30m&to=now"

