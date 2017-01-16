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

.PHONY: all production test tests coverage clean

all: production

production:
	@true

test:
	tox

tests: test
coverage: test

itest_%: package_%
	docker run -h fake.docker.hostname -v $(CURDIR):/work:rw docker-dev.yelpcorp.com/$*_yelp /bin/bash -c "/work/tests/ubuntu.sh"
	docker run -v $(CURDIR):/work:rw docker-dev.yelpcorp.com/$*_yelp chown -R $(UID):$(GID) /work

package_%:
	mkdir -p dist
	docker build -t "scl_$*_container" dockerfiles/$*
	docker run -h fake.docker.hostname -v $(CURDIR):/work:rw scl_$*_container /bin/bash -c "./package-python yelp1 . && mv *.deb dist/"
	docker run -v $(CURDIR):/work:rw docker-dev.yelpcorp.com/$*_yelp chown -R $(UID):$(GID) /work

clean:
	rm -rf dist/
	rm -rf build/
	rm -rf .tox
	rm -rf service_configuration_lib.egg-info/
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
