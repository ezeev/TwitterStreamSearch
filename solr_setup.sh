#!/usr/bin/env bash

mkdir solr

echo "Downloading Solr from http://www.eu.apache.org/dist/lucene/solr/5.3.1/solr-5.3.1.tgz"
curl -O http://www.eu.apache.org/dist/lucene/solr/5.3.1/solr-5.3.1.tgz

echo "Extracting Solr"
tar xf solr-5.3.1.tgz

mv solr-5.3.1 solr/
mv solr-5.3.1.tgz solr/

echo "Starting Solr"

solr/solr-5.3.1/bin/solr start -e cloud -noprompt

echo "Waiting 10 seconds for Solr to start"

sleep 10

echo "Creating tweettracks collection"

curl "http://localhost:8983/solr/admin/collections?action=CREATE&name=tweettracks&numShards=1&replicationFactor=2&maxShardsPerNode=1&collection.configName=gettingstarted"

echo "Indexing tweettracks.csv"

solr/solr-5.3.1/bin/post tweetstreams.csv -c tweettracks