#!/bin/bash
cd "$(dirname "$0")"

docker run -d --health-cmd "immuadmin status" --health-interval 10s --health-timeout 5s --health-retries 5 -v "$PWD"/certs/my.key.pem:/key.pem -p 3322:3322 codenotary/immudb:1.3.0 --signingKey=/key.pem
docker run -d --health-cmd "immuadmin status" --health-interval 10s --health-timeout 5s --health-retries 5 -v "$PWD"/certs/my.key.pem:/key.pem -p 3333:3322 codenotary/immudb:1.2.4 --signingKey=/key.pem
docker run -d --health-cmd "immuadmin status" --health-interval 10s --health-timeout 5s --health-retries 5 -v "$PWD"/certs/my.key.pem:/key.pem -p 3344:3322 codenotary/immudb:1.1.0 --signingKey=/key.pem