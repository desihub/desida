#!/bin/bash
# Licensed under a 3-clause BSD style license - see LICENSE.rst.
if [[ -z "${DESIDA}" ]]; then
    echo "ERROR: DESIDA is undefined!"
    exit 1
fi
source ${DESIDA}/bin/desida_library.sh
#
# Help message.
#
function usage() {
    local execName=$(basename $0)
    (
    echo "${execName} [-d DIR] [-h] [-j JOBS] [-v] [-V] SPECPROD DIRECTORY"
    echo ""
    echo "Create xfer jobs for SPECPROD."
    echo ""
    echo "    -d DIR    = Use this directory on HPSS (default 'desi/spectro/redux')."
    echo "    -h        = Print this message and exit."
    echo "    -j JOBS   = Use JOBS directory to write batch files (default ${DESI_ROOT}/users/${USER}/jobs)."
    echo "    -v        = Verbose mode. Print extra information."
    echo "    -V        = Version.  Print a version string and exit."
    echo ""
    echo "    SPECPROD  = Spectroscopic Production run name, e.g. 'iron'."
    echo "    DIRECTORY = Create backup jobs for this directory with in SPECPROD."
    ) >&2
}
hpss_dir='desi/spectro/redux'
jobs=${DESI_ROOT}/users/${USER}/jobs
verbose=false
while getopts d:hj:vV argname; do
    case ${argname} in
        d) hpss_dir=${OPTARG} ;;
        h) usage; exit 0 ;;
        j) jobs=${OPTARG} ;;
        v) verbose=true ;;
        V) version; exit 0 ;;
        *) usage; exit 1 ;;
    esac
done
shift $((OPTIND-1))
if (( $# < 1 )); then
    echo "ERROR: SPECPROD is required!" >&2
    exit 1
fi
if (( $# < 2 )); then
    echo "ERROR: DIRECTORY is required!" >&2
    exit 1
fi
export SPECPROD=$1
directory=$2
[[ -d ${jobs}/done ]] || mkdir -p ${jobs}/done
[[ -z "${DESI_SPECTRO_REDUX}" ]] && export DESI_SPECTRO_REDUX=/global/cfs/cdirs/desi/spectro/redux
if [[ ! -d ${DESI_SPECTRO_REDUX}/${SPECPROD} ]]; then
    echo "ERROR: ${DESI_SPECTRO_REDUX}/${SPECPROD} does not exist!" >&2
    exit 1
fi
for d in ${DESI_SPECTRO_REDUX}/${SPECPROD}/${directory}/*; do
    n=$(basename ${d})
    if [[ -d ${d} ]]; then
        job_name=redux_${SPECPROD}_$(tr '/' '_' <<<${directory})_${n}
        ${verbose} && echo "DEBUG: job_name=${job_name}" >&2
        cat > ${jobs}/${job_name}.sh <<EOT
#!/bin/bash
#SBATCH --account=desi
#SBATCH --qos=xfer
#SBATCH --constraint=cron
#SBATCH --time=12:00:00
#SBATCH --mem=10GB
#SBATCH --job-name=${job_name}
#SBATCH --output=${jobs}/%x-%j.log
#SBATCH --licenses=cfs,scratch,hpss
cd ${DESI_SPECTRO_REDUX}/${SPECPROD}/${directory}
hsi mkdir -p ${hpss_dir}/${SPECPROD}/${directory}
htar -cvf ${hpss_dir}/${SPECPROD}/${directory}/${job_name}.tar -H crc:verify=all ${n}
[[ \$? == 0 ]] && mv -v ${jobs}/${job_name}.sh ${jobs}/done
EOT
        ${verbose} && echo "DEBUG: chmod +x ${jobs}/${job_name}.sh" >&2
        chmod +x ${jobs}/${job_name}.sh
    else
        ${verbose} && echo "DEBUG: ${d} is not a directory, skipping." >&2
    fi
done
