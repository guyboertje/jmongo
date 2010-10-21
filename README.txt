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
  - limit, skip and sort
find_one
last_status
insert

2010-10-16, Guy Boertje ported the following...

count
update
save
find_and_modify
create_index
ensure_index
drop_index
drop_indexes
drop_collection
drop_database
database_names

Also the returned objects are BSON::OrderedHash objects to match those of the regular ruby mongo library

rough benchmarks

drop collection 'bm' before and after mongo run

require 'rubygems'
require 'jmongo'
#require 'mongo'
require 'benchmark'
n = 500
db = Mongo::Connection.new('127.0.0.1',37037).db('bm')
coll = db.collection('bm')
ids = []
docs = []
Benchmark.bm do |x|
  x.report("inserts:") do
    for i in 1..n
      d = {'n'=>i}
      ids << coll.insert(d)
    end
  end
  x.report("updates:") do
    ids.each do |id|
      coll.update({'_id'=>id},{'$set'=>{'a'=>'blah'}})
    end
  end
  s = {'n'=>{'$gt'=>0}}
  coll.find(s).each{|d| }
  coll.find(s).each{|d| }
  c = coll.find(s)
  x.report("after find all, iterate:") do
    c.each do |d|
      docs << d
    end
  end
end

$ ruby mongo_bm.rb
                          user     system      total        real
                inserts:  0.997000   0.000000   0.997000 (  0.997000)
                updates:  0.793000   0.000000   0.793000 (  0.793000)
after find all, iterate:  0.204000   0.000000   0.204000 (  0.204000)

$ ruby jmongo_bm.rb
                          user     system      total        real
                inserts:  0.434000   0.000000   0.434000 (  0.434000)
                updates:  0.475000   0.000000   0.475000 (  0.475000)
after find all, iterate:  0.145000   0.000000   0.145000 (  0.145000)


