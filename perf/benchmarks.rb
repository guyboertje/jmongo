#benchmarks
$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))

require 'rubygems' if RUBY_VERSION < '1.9.0'

if RUBY_PLATFORM =~ /java/
  require 'jmongo'
  puts "Using jmongo"
else
  require 'mongo'
  puts "Using mongo"
end

require "benchmark"
#require 'awesome_print'

unless defined? MONGO_PERF_DB
  MONGO_PERF_DB = 'ruby-perf-db'
end

unless defined? TEST_URI
  PERF_URI = "mongodb://localhost"
end

module Cfg
  def self.connection(options={})
    @con ||= Mongo::Connection.from_uri(PERF_URI)
  end

  def self.new_connection(uri, options={})
    Mongo::Connection.from_uri(PERF_URI, options)
  end

  def self.db
    @db ||= @con.db(MONGO_PERF_DB)
  end

  def self.version
    @con.server_version
  end

  def self.clear_all
    @db.collection_names.each do |n|
      @db.drop_collection(n) unless n =~ /system/
    end
  end

  def self.coll
    @db.collection("perf")
  end

  def self.coll_full_name
    "#{MONGO_PERF_DB}.test"
  end

  def self.clear
    @db.collection("perf").remove
  end
end

Cfg.connection
Cfg.db

2.times do |r|

  puts "", r == 0 ? "Rehearsing..." : "Starting benchmark..."

  Benchmark.bm(44) do |bm|

    Cfg.clear
    bm.report("Create 100k new docs no _id") do
      100_000.times do |n|
        Cfg.coll.insert(:a => n)
      end
    end

    Cfg.clear
    bm.report("Create 100k new docs w _id") do
      100_000.times do |n|
        Cfg.coll.insert(:_id => n, :a => n + 1)
      end
    end

    Cfg.clear
    bm.report("Batch insert 100 x 1k new docs no _id") do
      100.times do |b|
        arr = []
        1000.times do |n|
          arr << {:a => (b * 1_000) + n}
        end
        Cfg.coll.insert(arr)
      end
    end

    Cfg.clear
    bm.report("Batch insert 100 x 1k new docs w _id") do
      100.times do |b|
        arr = []
        1000.times do |n|
          i = (b * 1_000) + n
          arr << {:_id => i, :a => i}
        end
        Cfg.coll.insert(arr)
      end
    end

    bm.report("Update 100k docs w _id") do
      100_000.times do |n|
        Cfg.coll.update({:_id => n}, {:a => n + 2})
      end
    end

    Cfg.clear
    bm.report("Safe insert 100 x 1k new docs w _id") do
      100.times do |b|
        arr = []
        1000.times do |n|
          i = (b * 1_000) + n
          arr << {:_id => i, :a => i}
        end
        Cfg.coll.insert(arr, :safe => {:w=>1,:fsync=>true})
      end
    end
  end
end
