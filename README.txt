Project JMongo

The Mongo project provides native bindings for most popular languages. For the ruby language,
the project has a pure ruby execution path and a C extension to handle some of the heavy
lifting. Unfortunately, the performance of the pure ruby path under jruby is less than 
stellar. Additionally, the C extension isn't compatible with jruby so we aren't able to take
advantage of any native code boost.

JMongo solves this problem by putting a thin ruby wrapper around the mongo-java-driver. The
goal is to provide a drop-in replacement for the mongo and bson gems along with complete
API compatibility.

The initial version of this gem only wraps enough functionality to cover my personal use-cases.
I encourage project forking.

INSTALLATION
  % gem build jmongo.gemspec
  % gem install jmongo-0.1.0.gem


PROGRESS
The following methods have been ported and have at least basic functionality.

find
find_one
last_status
insert