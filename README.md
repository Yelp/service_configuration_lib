# Service Configuration Lib

service_configuration_lib is a Python interface to Yelp's SOA architecture.

WARNING: PaaSTA has been running in production at Yelp for more than a year, and
has a number of "Yelpisms" still lingering in the codebase. We have made efforts
to excise them, but there are bound to be lingering issues. Please help us by
opening an issue or better yet a pull request.

You can learn more about Yelp's SOA architecture by watching
[this](https://t.co/5khQ5KHDWL) video.

## Building

You can build service_configuration_lib as a deb by using the provided `package_python` script:

Example usage:

```
$ ./package-python myversion .

Created deb package {:path=>"/home/robj/service_configuration_lib/python-service-configuration-lib_0.10.0-myversion_all.deb"}

```
