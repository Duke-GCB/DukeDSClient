set -e

export DUKE_DATA_SERVICE_AUTH=$1
USERNAME=$2
USER_EMAIL=$3

if [ "$DUKE_DATA_SERVICE_AUTH" == "" -o "$USERNAME" == "" -o "$USER_EMAIL" == "" ]
then
   echo "Usage: $0 <AUTH> <USERNAME> <USER_EMAIL>"
   exit 1
fi

export DUKE_DATA_SERVICE_AUTH=$1
USERNAME=$2
USER_EMAIL=$3

PROJECT_PREFIX="int_test"

echo "python unit tests"
python setup.py -q test

echo "python3 unit tests"
python3 setup.py -q test

export PROJ="python$PROJECT_PREFIX"
echo "test upload $PROJ"
python -m ddsc upload -p $PROJ ddsc
python -m ddsc add_user -p $PROJ --email $USER_EMAIL

export PROJ="python3$PROJECT_PREFIX"
echo "test upload $PROJ"
python3 -m ddsc upload -p $PROJ ddsc
python -m ddsc add_user -p $PROJ --user $USERNAME

echo "Success check data on portal"
