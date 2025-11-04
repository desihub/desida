#!/bin/bash
mock_root=/global/cfs/cdirs/desi/public/dr1/survey/catalogs/dr1/mocks
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
            jobName=dr1_survey_catalogs_dr1_mocks_${mock}_${obs}_${ver}_${d}
            if [[ -f ./${jobName}.sh ]]; then
               rm -f ./${jobName}.sh
            fi
            cat > ./${jobName}.sh <<EOSLURM
#!/bin/bash
#SBATCH --account=desi
#SBATCH --qos=xfer
#SBATCH --constraint=cron
#SBATCH --time=12:00:00
#SBATCH --mem=10GB
#SBATCH --job-name=${jobName}
#SBATCH --output=/global/cfs/cdirs/desicollab/users/desi/jobs/%x-%j.log
#SBATCH --licenses=cfs,hpss
# set -o xtrace
cfs_root=/global/cfs/cdirs/desi/public
hpss_root=desi/public
job_dir=/global/cfs/cdirs/desicollab/users/\${USER}/jobs
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

        chmod +x ./${jobName}.sh
        done
    done
done
