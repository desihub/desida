#!/bin/bash
for d in BGS-BRIGHT ELG LRG QSO; do
    for filename in ${d}/*clustering.h5; do
        dd=${d}
        [[ "${d}" == "BGS-BRIGHT" ]] && dd=BGS
        b=$(basename ${filename})
        n=$(cut -d_ -f3 <<<${b})
        l=$(echo -n ${n} | wc -m)
        while (( l < 4 )); do
            n="0${n}"
            l=$(echo -n ${n} | wc -m)
        done
        # echo /bin/mv -v ${filename} "${d}/GLAM-Uchuu_${dd}_${n}_DR2_cut_sky_clustering.h5"
        /bin/mv -v ${filename} "${d}/GLAM-Uchuu_${dd}_${n}_DR2_cut_sky_clustering.h5"
    done
done
