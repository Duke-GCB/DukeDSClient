set -e

USERNAME=$1
USER_EMAIL=$2
PROJECT_PREFIX="int_test"
export PROJ="python$PROJECT_PREFIX"

if [ "$USERNAME" == "" -o "$USER_EMAIL" == "" ]
then
   echo "Usage: $0 <USERNAME> <USER_EMAIL>"
   exit 1
fi

echo "test upload $PROJ"
python3 -m ddsc upload -p $PROJ ddsc/
python3 -m ddsc add-user -p $PROJ --email $USER_EMAIL

echo "Waiting for DukeDS background processing"
sleep 30

echo "test download $PROJ"
# test filename conversion
python3 -m ddsc download -p $PROJ $PROJ
echo "differences:"
diff --brief -r ddsc/ $PROJ/ddsc/
rm -rf $PROJ

echo "SDK testing"
python3 -c 'import DukeDS; DukeDS.upload_file("pythonint_test", "README.md")'
python3 -c 'import DukeDS; DukeDS.download_file("pythonint_test", "README.md", "/tmp/README.md")'
echo "differences:"
diff README.md /tmp/README.md

ddsclient delete -p pythonint_test --force
