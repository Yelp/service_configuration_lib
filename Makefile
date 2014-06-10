PYTHON_PKGNAME = service_configuration_lib
UID:=`id -u`
GID:=`id -g`
DOCKER_RUN:=docker run -t -v  $(CURDIR):/work:rw lucid_container

.PHONY: all production test tests coverage clean

all: production

production:
	@true

test:
	tox

tests: test
coverage: test

itest_lucid: package_lucid
	$(DOCKER_RUN) /bin/bash -c "dpkg -i /work/dist/*.deb && python /work/tests/*.py"

package_lucid: test_lucid
	$(DOCKER_RUN) /bin/bash -c "./package-python yelp1 . && mv *.deb dist/"
	$(DOCKER_RUN) chown -R $(UID):$(GID) /work

test_lucid: build_lucid_docker
	$(DOCKER_RUN) bash -c "tox"
	$(DOCKER_RUN) chown -R $(UID):$(GID) /work

build_lucid_docker:
	[ -d dist ] || mkdir dist
	cd dockerfiles/lucid/ && docker build -t "lucid_container" .

clean:
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
	rm -rf dist/
	rm -rf .tox
