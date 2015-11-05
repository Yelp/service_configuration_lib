# Copyright 2015 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
UID:=`id -u`
GID:=`id -g`
LUCID_DOCKER_RUN:=docker run -h fake.docker.hostname -v $(CURDIR):/work:rw scl_lucid_container
TRUSTY_DOCKER_RUN:=docker run -h fake.docker.hostname -v $(CURDIR):/work:rw scl_trusty_container

.PHONY: all production test tests coverage clean

all: production

production:
	@true

test:
	tox

tests: test
coverage: test

itest_lucid: package_lucid
	$(LUCID_DOCKER_RUN) /bin/bash -c "/work/tests/ubuntu.sh"
	$(LUCID_DOCKER_RUN) chown -R $(UID):$(GID) /work

package_lucid: build_lucid_docker
	$(LUCID_DOCKER_RUN) /bin/bash -c "./package-python yelp1 . && mv *.deb dist/"
	$(LUCID_DOCKER_RUN) chown -R $(UID):$(GID) /work

build_lucid_docker:
	[ -d dist ] || mkdir dist
	cd dockerfiles/lucid/ && docker build -t "scl_lucid_container" .


itest_trusty: package_trusty
	$(TRUSTY_DOCKER_RUN) /bin/bash -c "/work/tests/ubuntu.sh"
	$(TRUSTY_DOCKER_RUN) chown -R $(UID):$(GID) /work

package_trusty: build_trusty_docker
	$(TRUSTY_DOCKER_RUN) /bin/bash -c "./package-python yelp1 . && mv *.deb dist/"
	$(TRUSTY_DOCKER_RUN) chown -R $(UID):$(GID) /work

build_trusty_docker:
	[ -d dist ] || mkdir dist
	cd dockerfiles/trusty/ && docker build -t "scl_trusty_container" .


clean:
	rm -rf dist/
	rm -rf build/
	rm -rf .tox
	rm -rf service_configuration_lib.egg-info/
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
