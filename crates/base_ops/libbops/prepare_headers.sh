SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
for cfile in $SCRIPTPATH/*.c
do
    $SCRIPTPATH/../../../tools/makeheaders $cfile || true
done
