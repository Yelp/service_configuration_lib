FROM docker-dev.yelpcorp.com/lucid_yelp
MAINTAINER Kyle Anderson <kwa@yelp.com>

RUN apt-get clean all
RUN apt-get update

RUN apt-get -y install dpkg-dev python-tox python-setuptools python-yaml
RUN apt-get -y install python-dev debhelper
RUN apt-get -y install ruby rubygems ruby-dev libopenssl-ruby rubygem-fpm
RUN gem install fpm

RUN ln -s /usr/bin/python /usr/local/bin/python

ENV HOME /work
ENV PWD /work
WORKDIR /work

