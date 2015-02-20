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
