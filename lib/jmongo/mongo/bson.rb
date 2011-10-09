module BSON
  # add missing BSON::ObjectId ruby methods
  java_import Java::OrgBsonTypes::ObjectId

  java_import Java::OrgBsonTypes::MaxKey
  java_import Java::OrgBsonTypes::MinKey
  java_import Java::OrgBsonTypes::Symbol

  OrderedHash = Java::ComMongodb::BasicDBObject
  BsonCode = Java::OrgBsonTypes::CodeWScope

  class ObjectId
    def self.from_string(str)
      v = is_valid?(str.to_s)
      raise BSON::InvalidObjectId, "illegal ObjectID format" unless v
      new(str.to_s)
    end

    def self.create_pk(doc)
      doc.has_key?(:_id) || doc.has_key?('_id') ? doc : doc.merge!('_id' => self.new)
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

  class MaxKey
    def ==(obj)
      obj.class == self.class
    end
  end

  class MinKey
    def ==(obj)
      obj.class == self.class
    end
  end

  class Code
    # Wrap code to be evaluated by MongoDB.
    #
    # @param [String] code the JavaScript code.
    # @param [Hash] a document mapping identifiers to values, which
    #   represent the scope in which the code is to be executed.
    def initialize(code, scope={})
      unless code.is_a?(String)
        raise ArgumentError, "BSON::Code must be in the form of a String; #{code.class} is not accepted."
      end
      @bson_code  = BsonCode.new(code, scope.to_bson)
    end

    def code
      @bson_code.code
    end

    def scope
      @bson_code.scope
    end

    def length
      code.length
    end

    def ==(other)
      self.class == other.class &&
        code == other.code && scope == other.scope
    end

    def inspect
      "<BSON::Code:#{object_id} @code=\"#{code}\" @scope=\"#{scope.inspect}\">"
    end

    def to_s
      code.to_s
    end

    def to_bson
      @bson_code
    end
    alias :to_bson_code :to_bson
  end

  class DBRef

    attr_reader :namespace, :object_id

    # Create a DBRef. Use this class in conjunction with DB#dereference.
    #
    # @param [String] a collection name
    # @param [ObjectId] an object id
    #
    # @core dbrefs constructor_details
    def initialize(namespace, object_id)
      @namespace = namespace
      @object_id = object_id
    end

    def to_s
      "ns: #{namespace}, id: #{object_id}"
    end

    def to_hash
      {"$ns" => @namespace, "$id" => @object_id }
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
