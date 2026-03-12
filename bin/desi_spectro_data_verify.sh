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
    echo "${execName} [-h] [-p] [-t] [-v] [-V] NIGHTS"
    echo ""
    echo "Verify data in DESI_SPECTRO_DATA, e.g. at Tucson."
    echo ""
    echo "         -h = Print this message and exit."
    echo "         -p = Set public permissions on files."
    echo "         -t = Test mode.  Do not actually make any changes. Implies -v."
    echo "         -v = Verbose mode. Print extra information."
    echo "         -V = Version.  Print a version string and exit."
    echo "     NIGHTS = A list of nights, typically corresponding to a data release."
    ) >&2
}
test=false
verbose=false
dir_perm='2750'
file_perm='0640'
while getopts hptvV argname; do
    case ${argname} in
        h) usage; exit 0 ;;
        p) dir_perm='2755'; file_perm='0644' ;;
        t) test=true; verbose=true ;;
        v) verbose=true ;;
        V) version; exit 0 ;;
        *) usage; exit 1 ;;
    esac
done
shift $((OPTIND-1))
#
# Check nights file.
#
if [[ $# < 1 ]]; then
    echo "ERROR: A list of nights is required!"
    exit 1
fi
nights=$1
if [[ ! -f ${nights} ]]; then
    echo "ERROR: Could not find ${nights}!"
    exit 1
fi
if [[ -z "${DESISYNC_HOSTNAME}" ]]; then
    echo "ERROR: DESISYNC_HOSTNAME is not set!"
    exit 1
fi
#
# Set up source and destination.
#
src="rsync://${DESISYNC_HOSTNAME}/desi/spectro/data"
# dst=/net/incoming/desi/spectro/data
dst=/net/archive/hlsp/desi/public/dr2/spectro/data
if ${test}; then
    dry_run='--dry-run'
else
    dry_run=''
fi
#
# Loop over nights.
#
for n in $(<${nights}); do
    resync=false
    night=$(basename ${n})
    #
    # Loop over exposures.
    #
    for e in ${dst}/${night}/*; do
        exposure=$(basename ${e})
        c=checksum-${exposure}.sha256sum
        if [[ -f ${e}/${c} ]]; then
            ${verbose} && echo "DEBUG: (cd ${e}; validate ${c})"
            (cd ${e}; validate ${c})
            if [[ $? == 0 ]]; then
                ${verbose} && echo "DEBUG: No problems detected with ${e}."
            else
                echo "ERROR: Checksum problem detected for ${e}!"
                resync=true
            fi
        else
            echo "ERROR: No checksum file in ${e}!"
            resync=true
        fi
    done
    if resync; then
        #
        # Permission unlock
        #
        ${verbose} && echo "DEBUG: chmod -R u+w ${dst}/${night}"
        ${test} || chmod -R u+w ${dst}/${night}
        #
        # Sync the night.
        #
        ${verbose} && echo "DEBUG: /usr/bin/rsync --archive --checksum --verbose --delete --delete-after --no-motd --password-file ${HOME}/.desi ${dry_run} ${src}/${night}/ ${dst}/${night}/"
        /usr/bin/rsync --archive --checksum --verbose --delete --delete-after --no-motd --password-file ${HOME}/.desi ${dry_run} ${src}/${night}/ ${dst}/${night}/
        #
        # Permission lock
        #
        ${verbose} && echo "DEBUG: find ${dst}/${night} -type f -exec chmod ${file_perm} \{\} \;"
        ${test} || find ${dst}/${night} -type f -exec chmod ${file_perm} \{\} \;
        ${verbose} && echo "DEBUG: find ${dst}/${night} -type f -exec chmod ${dir_perm} \{\} \;"
        ${test} || find ${dst}/${night} -type f -exec chmod ${dir_perm} \{\} \;
        ${verbose} && echo "DEBUG: chmod -R u-w ${dst}/${night}"
        ${test} || chmod -R u-w ${dst}/${night}
    fi
done
