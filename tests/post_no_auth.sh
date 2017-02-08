#!/bin/sh
if [[ "$OSTYPE" == "darwin"* ]]; then
    alias date='gdate'
fi
dt=`date +%s%6N`
curl -s -o /dev/null -w "%{http_code}" 'http://localhost:8080/raw/test' -H "Etag: \"$dt\"" -d 'zoozoo in the zoo'