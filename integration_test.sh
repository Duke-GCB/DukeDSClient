set -e

USERNAME=$1
USER_EMAIL=$2

if [ "$USERNAME" == "" -o "$USER_EMAIL" == "" ]
then
   echo "Usage: $0 <USERNAME> <USER_EMAIL>"
   exit 1
fi

USERNAME=$1
USER_EMAIL=$2

PROJECT_PREFIX="int_test"

echo "python unit tests"
python setup.py -q test

echo "python3 unit tests"
python3 setup.py -q test

export PROJ="python$PROJECT_PREFIX"
echo "test upload $PROJ"
python -m ddsc upload -p $PROJ ddsc/
python -m ddsc add-user -p $PROJ --email $USER_EMAIL

echo "test download $PROJ"
# test filename conversion
python -m ddsc download -p $PROJ
echo "differences:"
diff --brief -r ddsc/ $PROJ/ddsc/
rm -rf $PROJ

export PROJ2="python3$PROJECT_PREFIX"
echo "test upload $PROJ2"
python3 -m ddsc upload -p $PROJ2 ddsc/
python3 -m ddsc add-user -p $PROJ2 --user $USERNAME
python3 -m ddsc remove-user -p $PROJ2 --user $USERNAME

echo "test download $PROJ2"
rm -rf /tmp/$PROJ2
python3 -m ddsc download -p $PROJ /tmp/$PROJ2
echo "differences:"
diff --brief -r ddsc/ /tmp/$PROJ2/ddsc/

echo "Success check data on portal"
