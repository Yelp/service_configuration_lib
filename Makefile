UID:=`id -u`
GID:=`id -g`
DOCKER_RUN:=docker run -h fake.docker.hostname -v $(CURDIR):/work:rw lucid_container

.PHONY: all production test tests coverage clean

all: production

production:
	@true

test:
	tox

tests: test
coverage: test

itest_lucid: package_lucid
	$(DOCKER_RUN) /bin/bash -c "/work/tests/ubuntu.sh"
	$(DOCKER_RUN) chown -R $(UID):$(GID) /work

package_lucid: test_lucid
	rm -rf .tox
	$(DOCKER_RUN) /bin/bash -c "./package-python yelp1 . && mv *.deb dist/"
	$(DOCKER_RUN) chown -R $(UID):$(GID) /work

test_lucid: build_lucid_docker
	$(DOCKER_RUN) bash -c "tox"
	$(DOCKER_RUN) chown -R $(UID):$(GID) /work

build_lucid_docker:
	[ -d dist ] || mkdir dist
	cd dockerfiles/lucid/ && docker build -t "lucid_container" .

clean:
	rm -rf dist/
	rm -rf build/
	rm -rf .tox
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
