#!/bin/bash
set -Eeuo pipefail

dpkgArch="$(dpkg --print-architecture)"
case "$dpkgArch" in
	amd64) # https://github.com/docker-library/mongo/issues/485#issuecomment-891991814
		if ! grep -qE '^flags.* avx( .*|$)' /proc/cpuinfo; then
			{
				echo
				echo 'WARNING: MongoDB 5.0+ requires a CPU with AVX support, and your current system does not appear to have that!'
				echo '  see https://jira.mongodb.org/browse/SERVER-54407'
				echo '  see also https://www.mongodb.com/community/forums/t/mongodb-5-0-cpu-intel-g4650-compatibility/116610/2'
				echo '  see also https://github.com/docker-library/mongo/issues/485#issuecomment-891991814'
				echo
			} >&2
		fi
		;;

	arm64) # https://github.com/docker-library/mongo/issues/485#issuecomment-970864306
		# https://en.wikichip.org/wiki/arm/armv8#ARMv8_Extensions_and_Processor_Features
		# http://javathunderx.blogspot.com/2018/11/cheat-sheet-for-cpuinfo-features-on.html
		if ! grep -qE '^Features.* (fphp|dcpop|sha3|sm3|sm4|asimddp|sha512|sve)( .*|$)' /proc/cpuinfo; then
			{
				echo
				echo 'WARNING: MongoDB requires ARMv8.2-A or higher, and your current system does not appear to implement any of the common features for that!'
				echo '  applies to all versions ≥5.0, any of 4.4 ≥4.4.19, and any of 4.2 ≥4.2.19'
				echo '  see https://jira.mongodb.org/browse/SERVER-71772'
				echo '  see https://jira.mongodb.org/browse/SERVER-55178'
				echo '  see also https://en.wikichip.org/wiki/arm/armv8#ARMv8_Extensions_and_Processor_Features'
				echo '  see also https://github.com/docker-library/mongo/issues/485#issuecomment-970864306'
				echo
			} >&2
		fi
		;;
esac

exec "$@"
