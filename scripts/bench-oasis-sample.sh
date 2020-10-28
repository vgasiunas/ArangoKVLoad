#!/bin/bash
# Copy the public certificate of your deployment and save it to oasis-ca.pem
# Enter a correct host name and password
./arangokvload \
--time 20 --threads 8  --batchsize 1000 --numdocs 1000000 --numattrs 10 --attrlen 100 \
--user root --password XXXXXXX \
--host XXXXXX.arangodb.cloud --port 18529 --usessl --cacert oasis-ca.pem
