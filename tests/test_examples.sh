#!/bin/bash

FAILING=false
TESTS=()

for f in examples/*.py; do
  cp $f .
  pipenv run python $(basename $f) &
  DASH_PID=$!
  sleep 5
  RESP_CODE=$(curl --head --location --write-out %{http_code} --silent --output /dev/null http://127.0.0.1:8050/)
  # echo $RESP_CODE
  kill $DASH_PID
  rm $(basename $f)
  if [ "$RESP_CODE" != "200" ];
  then
    echo "$f FAILED! ($RESP_CODE)"
    FAILING=true
  else
    echo "$f Ok ($RESP_CODE)"
  fi
done

if [ FAILING == true ];
then
  exit 1
else
  exit 0
fi