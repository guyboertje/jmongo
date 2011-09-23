require 'timeout'
require 'java'

module JMongo
  import com.mongodb.BasicDBList
  import com.mongodb.BasicDBObject
  import com.mongodb.Bytes
  import com.mongodb.DB
  import com.mongodb.DBRef
  import com.mongodb.DBCollection
  import com.mongodb.DBCursor
  import com.mongodb.DBObject
  import com.mongodb.Mongo
  import com.mongodb.MongoOptions
  import com.mongodb.ServerAddress
  import com.mongodb.WriteConcern
  import com.mongodb.WriteResult
  import com.mongodb.MongoException
  import com.mongodb.MongoURI
end

class Java::ComMongodb::BasicDBObject
  if RUBY_PLATFORM == 'java' && JRUBY_VERSION =~ /(1\.[6-9]|[2-9]\.[0-9])..*/
    def hashify
      self.to_map.to_hash
    end
  else
    def hashify
      Hash[self.key_set.to_a.zip(self.values.to_a)]
    end
  end
  def get(key)
    self.java_send(:get,key.to_s)
  end
end

class Java::ComMongodb::BasicDBList
  def arrayify
    self.to_array
  end
end

#--------------------------------------------------
class String

  #:nodoc:
  def to_bson_code
    BSON::Code.new(self)
  end
end

module BSON
  # add missing BSON::ObjectId ruby methods
  class Java::OrgBsonTypes::ObjectId
    def self.from_string(str)
      v = is_valid?(str.to_s)
      raise BSON::InvalidObjectId, "illegal ObjectID format" unless v
      new(str.to_s)
    end

    def self.create_pk(doc)
      doc.has_key?(:_id) || doc.has_key?('_id') ? doc : doc.merge!(:_id => self.new)
    end

    def self.from_time(time, opts={})
      unique = opts.fetch(:unique, false)
      if unique
        self.new(time)
      else
        self.new([time.to_i,0,0].pack("NNN").to_java_bytes)
      end
    end

    #"data=", "decode64", "encode64", "decode_b", "b64encode" - shout out if these methods are needed

    def data
      self.to_byte_array.to_a.map{|x| x & 0xFF}
    end

    def clone
      self.class.new(self.to_byte_array)
    end

    def inspect
      "BSON::ObjectID('#{self.to_s}')"
    end

    def generation_time
      Time.at(self.get_time/1000).utc
    end
  end

  DBRef = Java::ComMongodb::DBRef
  MaxKey = Java::OrgBsonTypes::MaxKey

  class MaxKey
    def ==(obj)
      obj.class == self.class
    end
  end

  MinKey = Java::OrgBsonTypes::MinKey
  class MinKey
    def ==(obj)
      obj.class == self.class
    end
  end

  ObjectId = Java::OrgBsonTypes::ObjectId

  OrderedHash = Java::ComMongodb::BasicDBObject

  class Code < String
    # copied verbatim from ruby driver
        # Hash mapping identifiers to their values
    attr_accessor :scope, :code

    # Wrap code to be evaluated by MongoDB.
    #
    # @param [String] code the JavaScript code.
    # @param [Hash] a document mapping identifiers to values, which
    #   represent the scope in which the code is to be executed.
    def initialize(code, scope={})
      @code  = code
      @scope = scope

      unless @code.is_a?(String)
        raise ArgumentError, "BSON::Code must be in the form of a String; #{@code.class} is not allowed."
      end
    end

    def length
      @code.length
    end

    def ==(other)
      self.class == other.class &&
        @code == other.code && @scope == other.scope
    end

    def inspect
      "<BSON::Code:#{object_id} @data=\"#{@code}\" @scope=\"#{@scope.inspect}\">"
    end

    def to_bson_code
      self
    end
  end

  # Generic Mongo Ruby Driver exception class.
  class MongoRubyError < StandardError; end

  # Raised when MongoDB itself has returned an error.
  class MongoDBError < RuntimeError; end

  # This will replace MongoDBError.
  class BSONError < MongoDBError; end

  # Raised when given a string is not valid utf-8 (Ruby 1.8 only).
  class InvalidStringEncoding < BSONError; end

  # Raised when attempting to initialize an invalid ObjectId.
  class InvalidObjectId < BSONError; end
  class InvalidObjectID < BSONError; end

  # Raised when trying to insert a document that exceeds the 4MB limit or
  # when the document contains objects that can't be serialized as BSON.
  class InvalidDocument < BSONError; end

  # Raised when an invalid name is used.
  class InvalidKeyName < BSONError; end
end

module Mongo
  def self.logger(logger=nil)
    logger ? @logger = logger : @logger
  end
    # Generic Mongo Ruby Driver exception class.
  class MongoRubyError < StandardError; end

  # Raised when MongoDB itself has returned an error.
  class MongoDBError < RuntimeError; end

  # Raised when invalid arguments are sent to Mongo Ruby methods.
  class MongoArgumentError < MongoRubyError; end

  # Raised on failures in connection to the database server.
  class ConnectionError < MongoRubyError; end

  # Raised on failures in connection to the database server.
  class ReplicaSetConnectionError < ConnectionError; end

  # Raised on failures in connection to the database server.
  class ConnectionTimeoutError < MongoRubyError; end

  # Raised when a connection operation fails.
  class ConnectionFailure < MongoDBError; end

  # Raised when authentication fails.
  class AuthenticationError < MongoDBError; end

  # Raised when a database operation fails.
  class OperationFailure < MongoDBError; end

  # Raised when a socket read operation times out.
  class OperationTimeout < MongoDBError; end

  # Raised when a client attempts to perform an invalid operation.
  class InvalidOperation < MongoDBError; end

  # Raised when an invalid collection or database name is used (invalid namespace name).
  class InvalidNSName < RuntimeError; end

  # Raised when the client supplies an invalid value to sort by.
  class InvalidSortValueError < MongoRubyError; end
end
