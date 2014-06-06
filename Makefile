PYTHON_PKGNAME = service_configuration_lib

.PHONY: all production test tests coverage clean

all: production

production:
	@true

test:
	tox

tests: test
coverage: test

clean:
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
