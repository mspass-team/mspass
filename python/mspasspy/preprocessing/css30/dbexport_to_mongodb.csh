#!/bin/csh
set db=$1
echo Trying to build table of arrival data for database=$db
set outfile=${db}export.txt
echo Writing result to $outfile
dbjoin ${db}.event event origin assoc arrival \
  | dbsubset - "orid==prefor" \
  | dbselect - event.evid origin.lat origin.lon origin.depth origin.time origin.mb origin.ms assoc.sta assoc.phase arrival.iphase assoc.delta assoc.seaz assoc.esaz assoc.timeres arrival.time arrival.deltim \
  > $outfile
