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
ITERATION=yelp1

.PHONY: test tests coverage clean


test:
	tox

tests: test
coverage: test

itest_%: package_%
	docker run -h fake.docker.hostname -v $(CURDIR):/work:rw docker-dev.yelpcorp.com/$*_yelp /bin/bash -c "/work/tests/ubuntu.sh"
	docker run -v $(CURDIR):/work:rw docker-dev.yelpcorp.com/$*_yelp chown -R $(UID):$(GID) /work

package_%:
	mkdir -p dist
	docker run \
		-h fake.docker.hostname \
		-v $(CURDIR):/work:rw \
		docker-dev.yelpcorp.com/$*_pkgbuild \
		/bin/bash -c 'cd /work && \
			fpm --force \
				-s python -t deb \
				-m "Compute Infrastructure <compute-infra@yelp.com>" \
				--deb-user "root" --deb-group "root" \
				--python-pypi "https://pypi.yelpcorp.com/simple" \
				--python-install-lib "/usr/lib/python2.7/dist-packages" \
				--python-install-bin "/usr/bin" \
				--python-install-data "/usr" \
				--no-python-dependencies \
				--depends "python-yaml > 3.0" \
				--deb-no-default-config-files \
				--iteration="$(ITERATION)" . && \
			mv *.deb dist/ \
		'
	docker run -v $(CURDIR):/work:rw docker-dev.yelpcorp.com/$*_yelp chown -R $(UID):$(GID) /work

venv: requirements.txt setup.py tox.ini
	tox -e venv

clean:
	rm -rf .cache
	rm -rf dist/
	rm -rf build/
	rm -rf .tox
	rm -rf service_configuration_lib.egg-info/
	rm -rf venv
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
