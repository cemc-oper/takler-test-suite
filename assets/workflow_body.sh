name="${TAKLER_NAME##*/}"
case "$name" in
    prepare)
        printf '%s\n' 2 3 5 > "$SCN09_FLOW.left.input"
        printf '%s\n' 7 11 13 > "$SCN09_FLOW.right.input"
        ;;
    left|right)
        total=0
        while IFS= read -r value; do
            total=$((total + value))
        done < "$SCN09_FLOW.$name.input"
        printf '%s\n' "$total" > "$SCN09_FLOW.$name.sum"
        ;;
    merge)
        read -r left < "$SCN09_FLOW.left.sum"
        read -r right < "$SCN09_FLOW.right.sum"
        printf 'left=%s\nright=%s\ntotal=%s\n' "$left" "$right" "$((left + right))" > "$SCN09_FLOW.result.txt"
        ;;
    *) exit 2 ;;
esac
