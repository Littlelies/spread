#!/bin/sh
echo "Running test on $OSTYPE"
if [[ "$OSTYPE" == "darwin"* ]]; then
    alias date='gdate'
fi
dt=`date +%s%6N`
token="AAA"
curl 'http://localhost:8080/raw/test/image' -H "Etag: \"$dt\"" -H "Authorization: Bearer $token" --data-binary @pic.jpg
