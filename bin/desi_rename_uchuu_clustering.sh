#!/bin/bash
#
# src=${CFS}/desi/mocks/cai/LSS/DA2/mocks/GLAM-Uchuu_v2
src=${CFS}/desi/mocks/cai/LSS/DA2/mocks/glam_bgs_v2
# dst=${CFS}/desi/vac/dr2/glam-uchuu/v1.0/dark_tracers
dst=${CFS}/desi/vac/dr2/glam-uchuu/v1.0/BGS
dd=$(basename ${dst})
for altmtl in ${src}/altmtl*; do
    a=$(basename ${altmtl})
    aa=${dst}/${a}
    echo mkdir -p ${aa}
    [[ -d ${aa} ]] || mkdir -p ${aa}
    n=$(cut -dl -f3 <<<${a})
    for t in ${altmtl}/loa-v1/mock${n}/LSScats/*.txt; do
        tt=$(basename ${t})
        echo cp -a ${t} ${aa}
        [[ -f ${aa}/${tt} ]] || cp -a ${t} ${aa}
    done
    for t in ${altmtl}/loa-v1/mock${n}/LSScats/*.dat.h5; do
        tt=$(basename ${t})
        echo cp -a ${t} ${aa}
        [[ -f ${aa}/${tt} ]] || cp -a ${t} ${aa}
    done
    for k in $(seq 0 3); do
        for t in ${altmtl}/loa-v1/mock${n}/LSScats/*_${k}_clustering.ran.h5; do
            tt=$(basename ${t})
            echo cp -a ${t} ${aa}
            [[ -f ${aa}/${tt} ]] || cp -a ${t} ${aa}
        done
    done
    echo chmod 0640 ${aa}/*
    chmod 0640 ${aa}/*
    c=dr2_vac_dr2_glam-uchuu_v1.0_${dd}_${a}.sha256sum
    if [[ ! -f ${aa}/${c} ]]; then
        # echo pushd ${aa}
        pushd ${aa}
        echo "sha256sum *.h5 > ${c}"
        sha256sum *.h5 > ${c}
        echo "sha256sum *.txt >> ${c}"
        sha256sum *.txt >> ${c}
        # echo popd
        popd
    fi
done
