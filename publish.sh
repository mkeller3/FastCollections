# loop thru args to find nextVersion and assign to the nextVersion variable
for arg in "$@"; do
    case $arg in
        --nextVersion=*)
            nextVersion="${arg#*=}"
            ;;
    esac
done

# open the file api/version.py and write __version__ = "0.0.1"
sed -i "s/__version__ = .*/__version__ = \"$nextVersion\"/" api/version.py