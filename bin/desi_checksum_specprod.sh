#!/bin/bash
# Licensed under a 3-clause BSD style license - see LICENSE.rst.
#
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
    echo "${execName} [-h] [-j JOBS] [-s DIR] [-V] [-z VERSION] SPECPROD"
    echo ""
    echo "Checksum an entire spectroscopic reduction (SPECPROD) in preparation"
    echo "for tape backup."
    echo ""
    echo "Assuming files are on disk are in a clean, archival state, this script"
    echo "will create checksum files for the entire data set."
    echo ""
    echo "    -h         = Print this message and exit."
    echo "    -j JOBS    = Use JOBS directory to write batch files (default ${DESI_ROOT}/users/${USER}/jobs)."
    echo "    -s DIR     = Use DIR for temporary files (default ${SCRATCH})."
    echo "    -V         = Version. Print a version string and exit."
    echo "    -z VERSION = Version of zcatalog (default 'v2')."
    echo ""
    echo "    SPECPROD = Spectroscopic Production run name, e.g. 'iron'."
    ) >&2
}
#
# Create jobs.
#
function create_checksum_job() {
    local checksum_name=$1
    shift
    local checksum_dir=$(dirname ${checksum_name})
    local checksum_file=$(basename ${checksum_name})
    local job_name=${checksum_file%.sha256sum}
    if [[ "$@" == "." ]]; then
        local command="find . -type f -exec sha256sum \{\} \;"
    else
        local command="sha256sum $@"
    fi
    cat > ${jobs}/${job_name}.sh <<EOT
#!/bin/bash
#SBATCH --account=desi
#SBATCH --qos=xfer
#SBATCH --constraint=cron
#SBATCH --time=4:00:00
#SBATCH --job-name=${job_name}
#SBATCH --output=${jobs}/%x-%j.log
#SBATCH --licenses=cfs,scratch
source /global/common/software/desi/desi_environment.sh main
module load desida desiBackup
source ${DESIDA}/bin/desida_library.sh
set -o xtrace
shopt -s extglob
cd ${checksum_dir}
if [[ -f ${checksum_file} ]]; then
    validate ${checksum_file} && mv ${jobs}/${job_name}.sh ${jobs}/done
else
    ${command} > ${scratch}/${checksum_file}
    [[ \$? == 0 ]] && unlock_and_move ${scratch}/${checksum_file} && mv ${jobs}/${job_name}.sh ${jobs}/done
fi
EOT
    chmod +x ${jobs}/${job_name}.sh
}
#
# Get options.
#
jobs=${DESI_ROOT}/users/${USER}/jobs
scratch=${SCRATCH}
zcat_version=v2
thesePix=spectra
while getopts hj:s:V argname; do
    case ${argname} in
        h) usage; exit 0 ;;
        j) jobs=${OPTARG} ;;
        s) scratch=${OPTARG} ;;
        V) version; exit 0 ;;
        z) zcat_version=${OPTARG} ;;
        *) usage; exit 1 ;;
    esac
done
shift $((OPTIND-1))
if [[ $# < 1 ]]; then
    echo "ERROR: SPECPROD must be defined on the command-line!" >&2
    exit 1
fi
export SPECPROD=$1
[[ -d ${jobs}/done ]] || mkdir -p ${jobs}/done
if [[ ! -d ${DESI_SPECTRO_REDUX}/${SPECPROD} ]]; then
    echo "ERROR: ${DESI_SPECTRO_REDUX}/${SPECPROD} does not exist!" >&2
    exit 1
fi
#
# Top-level files
#
home=${DESI_SPECTRO_REDUX}/${SPECPROD}
cd ${home}
create_checksum_job ${DESI_SPECTRO_REDUX}/${SPECPROD}/redux_${SPECPROD}.sha256sum exposures-${SPECPROD}.\* tiles-${SPECPROD}.\* inventory-${SPECPROD}.\*
#
# tilepix.* files in healpix directory, or equivalent files in spectra directory.
#
if [[ -d healpix ]]; then
    thesePix=healpix
    create_checksum_job ${DESI_SPECTRO_REDUX}/${SPECPROD}/healpix/redux_${SPECPROD}_healpix.sha256sum tilepix.\*
elif [[ -d spectra ]]; then
    for SURVEY in spectra/*; do
        survey=$(basename ${SURVEY})
        for PROGRAM in ${SURVEY}/*; do
            program=$(basename ${PROGRAM})
            create_checksum_job ${DESI_SPECTRO_REDUX}/${SPECPROD}/spectra/${survey}/${program}/redux_${SPECPROD}_spectra_${survey}_${program}.sha256sum \*.fits \*.json
        done
    done
else
    thesePix=''
    echo "WARNING: Could not find ${DESI_SPECTRO_REDUX}/${SPECPROD}/healpix or spectra!" >&2
fi
#
# calibnight, exposure_tables
#
for d in calibnight exposure_tables nightqa; do
    if [[ -d ${d} ]]; then
        for NIGHT in ${d}/*; do
            night=$(basename ${NIGHT})
            create_checksum_job ${DESI_SPECTRO_REDUX}/${SPECPROD}/${d}/${night}/redux_${SPECPROD}_${d}_${night}.sha256sum \*
        done
    else
        echo "WARNING: Could not find ${DESI_SPECTRO_REDUX}/${SPECPROD}/${d}!" >&2
    fi
done
#
# processing_tables, run, zcatalog
#
create_checksum_job ${DESI_SPECTRO_REDUX}/${SPECPROD}/processing_tables/redux_${SPECPROD}_processing_tables.sha256sum \*
create_checksum_job ${DESI_SPECTRO_REDUX}/${SPECPROD}/run/redux_${SPECPROD}_run.sha256sum .
if (( ${zcat_version:1} > 1 )); then
    for z in $(find zcatalog -type d); do
        if [[ $(basename ${z}) == "exp_fibermap" ]]; then
            c='*'
        else
            c='!(exp_fibermap)'
        fi
        has_files=$(find ${z} -maxdepth 1 -type f)
        if [[ -n "${has_files}" ]]; then
            s=redux_${SPECPROD}_$(tr '/' '_' <<<${z}).sha256sum
            create_checksum_job ${DESI_SPECTRO_REDUX}/${SPECPROD}/${z}/${s} "${c}"
        fi
    done
else
    create_checksum_job ${DESI_SPECTRO_REDUX}/${SPECPROD}/zcatalog/${zcat_version}/redux_${SPECPROD}_zcatalog_${zcat_version}.sha256sum '!(logs)'
    create_checksum_job ${DESI_SPECTRO_REDUX}/${SPECPROD}/zcatalog/${zcat_version}/logs/redux_${SPECPROD}_zcatalog_${zcat_version}_logs.sha256sum \*
fi
#
# exposures, preproc, etc.
#
for d in dark_preproc exposures preproc; do
    if [[ -d ${d} ]]; then
        for NIGHT in ${d}/*; do
            night=$(basename ${NIGHT})
            for EXPID in ${NIGHT}/*; do
                expid=$(basename ${EXPID})
                if is_empty ${EXPID}; then
                    echo "WARNING: ${EXPID} is empty." >&2
                else
                    create_checksum_job ${DESI_SPECTRO_REDUX}/${SPECPROD}/${EXPID}/redux_${SPECPROD}_$(tr '/' '_' <<<${EXPID}).sha256sum \*
                fi
            done
        done
    else
        echo "WARNING: Could not find ${DESI_SPECTRO_REDUX}/${SPECPROD}/${d}!" >&2
    fi
done
#
# healpix, tiles
#
for d in ${thesePix} tiles; do
    for GROUP in ${d}/*; do
        # group=$(basename ${GROUP})
        if [[ -d ${GROUP} ]]; then
            for dd in $(find ${GROUP} -type d); do
                if [[ ${dd} =~ ^spectra/(cmx|main|special|sv1|sv2|sv3)/(backup|bright|dark|other)$ ]]; then
                    echo "INFO: Skipping ${dd} which should already have a special checksum script." >&2
                else
                    if [[ $(basename ${dd}) == "logs" ]]; then
                        c='*'
                    else
                        c='!(logs)'
                    fi
                    has_files=$(find ${dd} -maxdepth 1 -type f)
                    if [[ -n "${has_files}" ]]; then
                        s=redux_${SPECPROD}_$(tr '/' '_' <<<${dd}).sha256sum
                        create_checksum_job ${DESI_SPECTRO_REDUX}/${SPECPROD}/${dd}/${s} "${c}"
                    fi
                fi
            done
        fi
    done
done
