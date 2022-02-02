#!/bin/bash -e
REPO_ROOT=$(git rev-parse --show-toplevel)

function usage {
        echo "Usage: $(basename $0) [-k private-gcp-key] [-u gcp-user] <ansible_cmd_args>"
        echo ""
        echo " Execute the ansible command using the correct inventory with the given gcp user and key"
        echo " Defaults the gcp user and key to the standard values."
        echo ""
        echo " Example: $(basename $0) -- gcp_perf -a 'systemctl start jms-bridge-perf' --become"
        echo ""
        echo "    -h )"
        echo "        Show usage"
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
        echo ""
}

function errout {
  echo "$1" >&2
}

function abs_path {
      echo "$(cd "$(dirname "$1")"; pwd)/$(basename "$1")"
}

# list of arguments expected in the input
optstring=":hu:k:"
_clean=0
_scenario=''
_props=''
_gcpkey="$HOME/.ssh/google_compute_engine"
_gcpuser=$(gcloud info | grep Account: | sed -e "s#Account:.*\[\(.*\)\].*#\1#" | sed -e "s#[^a-zA-Z1-9]#_#g")
_playbook='site.yml'

while getopts ${optstring} arg; do
  case ${arg} in
    h)
      usage
      exit 0
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
if ! command -v gcloud &>/dev/null; then _errs=1; errout "ERROR: gcloud is required"; fi
if [ "x$_gcpuser" == 'x' ]; then _errs=1; errout "ERROR: -u, GCP user is required"; fi
if [ $_errs -eq 1 ]; then usage; exit 1; fi

# Ansible
pushd "${REPO_ROOT}/jms-bridge-perf/ansible" &>/dev/null || exit 1
errout "Executing ansible"
shift || true
ANSIBLE_HOST_KEY_CHECKING=False ansible \
  -u "${_gcpuser}" \
  --private-key "${_gcpkey}" \
  -i inventory.gcp.yml "$@"


