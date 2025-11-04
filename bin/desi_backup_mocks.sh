#!/bin/bash
# Licensed under a 3-clause BSD style license - see LICENSE.rst.
release=dr1
jobs=${CFS}/desicollab/users/${USER}/jobs
mock_root=${CFS}/desi/public/${release}/survey/catalogs/${release}/mocks
for mock in AbacusSummit EZmock; do
    for obs in bright dark; do
        ver='v1'
        declare -a backups
        if [[ "${mock}" == "AbacusSummit" ]]; then
            if [[ "${obs}" == "dark" ]]; then
                ver='v4.2'
            fi
            backups+=('files')
            for k in ${mock_root}/${mock}/${obs}/${ver}/altmtl*; do
                b=$(basename ${k})
                backups+=(${b})
            done
        fi
        for k in ${mock_root}/${mock}/${obs}/${ver}/mock*; do
            b=$(basename ${k})
            backups+=(${b})
        done
        for d in ${backups[@]}; do
            jobName=${release}_survey_catalogs_${release}_mocks_${mock}_${obs}_${ver}_${d}
            echo "DEBUG: jobName=${jobName}"
            if [[ -f ${jobs}/${jobName}.sh ]]; then
               echo "DEBUG: rm -f ${jobs}/${jobName}.sh"
               rm -f ${jobs}/${jobName}.sh
            fi
            cat > ${jobs}/${jobName}.sh <<EOSLURM
#!/bin/bash
#SBATCH --account=desi
#SBATCH --qos=xfer
#SBATCH --constraint=cron
#SBATCH --time=12:00:00
#SBATCH --mem=10GB
#SBATCH --job-name=${jobName}
#SBATCH --output=${jobs}/%x-%j.log
#SBATCH --licenses=cfs,hpss
# set -o xtrace
cfs_root=\${CFS}/desi/public
hpss_root=desi/public
job_dir=${jobs}
htar_path=\$(tr '_' '/' <<<\${SLURM_JOB_NAME})
htar_dir=\$(dirname \${htar_path})
htar_subdir=\$(basename \${htar_path})
if [[ "\${htar_subdir}" == "files" ]]; then
    htar_subdir='*.fits *.sha256sum'
fi
cd \${cfs_root}/\${htar_dir}
hsi mkdir -p \${hpss_root}/\${htar_dir}
htar -cvf \${hpss_root}/\${htar_dir}/\${SLURM_JOB_NAME}.tar -H crc:verify=all \${htar_subdir}
[[ \$? == 0 ]] && mv -v \${job_dir}/\${SLURM_JOB_NAME}.sh \${job_dir}/done
EOSLURM

        echo "DEBUG: chmod +x ${jobs}/${jobName}.sh"
        chmod +x ${jobs}/${jobName}.sh
        done
    done
done
