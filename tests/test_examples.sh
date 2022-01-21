#!/bin/bash

FAILING=false

for f in examples/*.py; do
  cp $f .
  pipenv run python $(basename $f) &
  DASH_PID=$!
  sleep 5
  RESP_CODE=$(curl --head --location --write-out %{http_code} --silent --output /dev/null http://127.0.0.1:8050/)
  # echo $RESP_CODE
  # kill -9 $DASH_PID
  kill `lsof -w -n -i tcp:8050 | awk '$2!="PID" {print $2;}'`
  rm $(basename $f)
  if [ "$RESP_CODE" != "200" ];
  then
    echo "$f FAILED! ($RESP_CODE)"
    FAILING=true
  else
    echo "$f Ok ($RESP_CODE)"
  fi
done

if [ "$FAILING" == "true" ];
then
  echo "ERROR"
  exit 1
else
  echo "SUCCESS"
  exit 0
fi