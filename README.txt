Project JMongo

The Mongo project provides native bindings for most popular languages. For the ruby language,
the project has a pure ruby execution path and a C extension to handle some of the heavy
lifting. Unfortunately, the performance of the pure ruby path under jruby is less than
stellar. Additionally, the C extension isn't compatible with jruby so we aren't able to take
advantage of any native code boost.

JMongo solves this problem by putting a thin ruby wrapper around the 10gen mongo-java-driver. The
goal is to provide a drop-in replacement for the mongo and bson gems along with complete
API compatibility.

The repo was was forked from Chuck Remes's (now deleted) repo.

INSTALLATION (from Rubygems.org)
  % gem install jmongo

USAGE
  * Use jruby with 1.9 compatibility turned on JRUBY_OPTS='--1.9'

PROGRESS
Almost all of the Ruby driver API is implemented

The the Ruby driver tests have been brought over and converted to be MiniTest based and
the collection and cursor test suites pass. NOTE: a few (2/3) tests have been skipped, you should look at
them to see if they affect you.

The Mongoid rspec functional suite runs 2607 examples with 28 failures when using JMongo
My Mongoid repo was forked after this commit (so newer funtionality/spec will be missing)
  commit 6cc97092bc10535b8b65647a3d14b10ca1b94c8c
  Author: Bernerd Schaefer <bj.schaefer@gmail.com>
  Date:   Tue Jun 28 12:59:34 2011 +0200

The failures are classed in this way:
  * Different Exception class being raised for BSON invalid keys
  * Managing Replica Sets directly
  * Managing Connection Pools directly
  * XML serialization
  * Ruby RegExp to BSON encode and decode

I will fix these problems in due course

Please note that the java driver handles the Replica Sets and connection pools
If you are using Replica Sets and want to use JMongo you should be OK if you use a URI to connect.
JMongo lets the Java driver handle reading from slaves and writing to master, although YMMV as I have not
tested JMongo with Replica Sets yet.
