#!/bin/bash
set -e

SCRIPTS="all_nodes_that_run
dump_service_configuration_yaml
services_deployed_here
services_needing_puppet_help
services_that_run_here
services_using_ssl"

if dpkg -i /work/dist/*.deb; then
  echo "Package installed correctly..."
else
  echo "Dpkg install failed!"
  exit 1
fi

if tox >/dev/null; then
  echo "Library can be imported..."
else
  echo "Package installed but library failed to import!"
  exit 1
fi

for scr in $SCRIPTS
do
  which $scr >/dev/null || echo "$scr failed to install!"
done

echo "All scripts are in the path..."
echo "Everything worked! Exiting..."
