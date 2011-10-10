$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'rubygems' if RUBY_VERSION < '1.9.0' && ENV['C_EXT']
require 'jmongo'
#require 'test/unit'
require 'awesome_print'

require 'minitest/autorun'

def silently
  warn_level = $VERBOSE
  $VERBOSE = nil
  result = yield
  $VERBOSE = warn_level
  result
end

def apr(obj, prefix = '=====')
  puts prefix
  ap obj
  puts '====='
end

begin
  require 'rubygems' if RUBY_VERSION < "1.9.0" && !ENV['C_EXT']
  silently { require 'shoulda' }
  silently { require 'mocha' }
rescue LoadError
  puts <<MSG

This test suite requires shoulda and mocha.
You can install them as follows:
  gem install shoulda
  gem install mocha

MSG

  exit
end

require 'bson_ext/cbson' if !(RUBY_PLATFORM =~ /java/) && ENV['C_EXT']

unless defined? MONGO_TEST_DB
  MONGO_TEST_DB = 'ruby-test-db'
end

unless defined? TEST_PORT
  TEST_PORT = ENV['MONGO_RUBY_DRIVER_PORT'] ? ENV['MONGO_RUBY_DRIVER_PORT'].to_i : Mongo::Connection::DEFAULT_PORT
end

unless defined? TEST_HOST
  TEST_HOST = ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost'
end

unless defined? TEST_URI
  TEST_URI = "mongodb://localhost"
end

module Cfg
  def self.connection(options={})
    #@con ||= Mongo::Connection.from_uri(TEST_URI)
    @con ||= Mongo::Connection.new(TEST_HOST, TEST_PORT)
  end

  def self.conn
    connection
  end

  def self.new_connection(options={})
    Mongo::Connection.from_uri(TEST_URI, options)
  end

  def self.host_port
    "#{TEST_HOST}:#{TEST_PORT}"
  end

  def self.mongo_host
    TEST_HOST
  end

  def self.mongo_port
    TEST_PORT
  end

  def self.db
    @db ||= @con.db(MONGO_TEST_DB)
  end

  def self.version
    @con.server_version
  end

  def self.clear_all
    @db.collection_names.each do |n|
      @db.drop_collection(n) unless n =~ /system/
    end
  end

  def self.test
    @db.collection("test")
  end

  def self.coll
    test
  end

  def self.coll_full_name
    "#{MONGO_TEST_DB}.test"
  end
end

class MiniTest::Unit::TestCase
  include Mongo
  include BSON 

  def new_mock_socket(host='localhost', port=27017)
    socket = Object.new
    socket.stubs(:setsockopt).with(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
    socket.stubs(:close)
    socket
  end

  def new_mock_db
    db = Object.new
  end

  def assert_not_nil arg, msg = ""
    refute_nil arg, msg
  end

  def assert_raise_error(klass, message)
    begin
      yield
    rescue => e
      assert_equal klass, e.class
      assert e.message.include?(message), "#{e.message} does not include #{message}."
    else
      flunk "Expected assertion #{klass} but none was raised."
    end
  end
end
