function _complete_wb() {

    LS="$(wbls ${COMP_WORDS[-1]})"
    IFS=', '
    CHILDREN=(${LS})
    IFS=$'\n'
    CANDIDATES=($(compgen -W '${CHILDREN[*]}' -- "${COMP_WORDS[-1]}"))
    IFS=' '
    if [[ ${#CANDIDATES[*]} -eq 0 ]]; then
        COMPREPLY=()
    else
        COMPREPLY=($(printf '%q/\n' "${CANDIDATES[*]}"))
    fi

    # IFS='/' read -ra SEGMENTS <<<"${COMP_WORDS[-1]}"
    # for i in "${SEGMENTS[@]}"; do
    #     echo $i
    # done

}
complete -F _complete_wb wbls
