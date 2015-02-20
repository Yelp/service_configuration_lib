FROM docker-dev.yelpcorp.com/trusty_yelp
MAINTAINER Kyle Anderson <kwa@yelp.com>

RUN apt-get clean all
RUN apt-get update

RUN apt-get -y install dpkg-dev python-tox python-setuptools python-yaml
RUN apt-get -y install python-dev debhelper
RUN apt-get -y install ruby ruby-dev
RUN gem install fpm

RUN ln -s /usr/bin/python /usr/local/bin/python
RUN ln -s /usr/local/bin/fpm /usr/bin/fpm

ENV HOME /work
ENV PWD /work
WORKDIR /work
