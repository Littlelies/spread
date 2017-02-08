#!/bin/sh
if [[ "$OSTYPE" == "darwin"* ]]; then
    alias date='gdate'
fi
dt=`date +%s%6N`
token="AAA"
curl -s 'http://localhost:8080/raw/test/test1' -H "Etag: \"$dt\"" -H "Authorization: Bearer $token"
