#!/bin/sh
if [[ "$OSTYPE" == "darwin"* ]]; then
    alias date='gdate'
fi
dt=`date +%s%6N`
token="AAA"
curl -s -o /dev/null -w "%{http_code}" 'http://localhost:8080/raw/test/test2' -H "Etag: \"$dt\"" -H "Authorization: Bearer $token"
