require 'java'

module JMongo
  import com.mongodb.BasicDBList
  import com.mongodb.BasicDBObject
  import com.mongodb.Bytes
  import com.mongodb.DB
  import com.mongodb.DBCollection
  import com.mongodb.DBCursor
  import com.mongodb.DBObject
  import com.mongodb.Mongo
  import com.mongodb.MongoOptions
  import com.mongodb.ServerAddress
  import com.mongodb.WriteConcern
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
end

class Java::ComMongodb::BasicDBList
  def arrayify
    self.to_map.to_hash.values.to_a
  end
end

# add missing BSON::ObjectId ruby methods
class Java::OrgBsonTypes::ObjectId
  def self.create_pk(doc)
    doc.has_key?(:_id) || doc.has_key?('_id') ? doc : doc.merge!(:_id => self.new)
  end

#"data=", "decode64", "encode64", "decode_b", "b64encode" - shout out if these methods are needed

  def data
    self.to_byte_array.to_a.map{|x| x & 0xFF}
  end
  def inspect
    "BSON::ObjectID('#{self.to_s}')"
  end
  def generation_time
    Time.at(self.get_time/1000).utc
  end
end

module BSON

  class Java::OrgBsonTypes::ObjectId
    def self.from_string(str)
      new(str.to_s)
    end
  end

  ObjectId = Java::OrgBsonTypes::ObjectId
  OrderedHash = Java::ComMongodb::BasicDBObject

  class Code < String
    # copied verbatim from ruby driver
    # Hash mapping identifiers to their values
    attr_accessor :scope
    def initialize(code, scope={})
      super(code)
      @scope = scope
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

  # Raised when trying to insert a document that exceeds the 4MB limit or
  # when the document contains objects that can't be serialized as BSON.
  class InvalidDocument < BSONError; end

  # Raised when an invalid name is used.
  class InvalidKeyName < BSONError; end

end

__END__

  module BasicDBObjectExtentions
    def keys
      self.key_set.to_a
    end
    def values
      self.java_send(:values).to_a
    end
    def merge!(other)
      self.put_all(other)
      self
    end
    def merge(other)
      obj = new
      obj.merge!(self)
      obj.merge!(other)
    end
    def put(key,val)
      self.java_send(:put,key.to_s,val)
    end
    def get(key)
      self.java_send(:get,key.to_s)
    end
    def [](key)
      self.get(key)
    end
  end

    include JMongo::BasicDBObjectExtentions

  # ["ordered_keys=", "invert", "rehash", "replace"] - these methods don't make much sense for a BasicBSONObject and derivative classes

    alias :update :merge!
    alias :each_pair :each
    alias :length :size

    def hashify
      Hash[self.keys.zip(self.values)]
    end

    def ordered_keys
      self.keys
    end

    def index(val)
      ix = self.values.to_a.index(val)
      return nil unless ix
      self.key_set.to_a[ix]
    end

    def reject!(&block)
      n = self.size
      self.each do |k,v|
        if yield(k, v)
          delete(k)
        end
      end
      return nil if n == self.size
      self
    end

    def each_key
      keys_ = self.key_set.to_a
      while keys_.length > 0
        yield keys_.shift
      end
    end

    def each_value
      vals = self.values
      while vals.length > 0
        yield vals.shift
      end
    end

    def fetch(key,default=nil)
      v = self.get(key)
      return v if !!(v)
      return yield(key) if block_given?
      return default unless default.nil?
      raise "index not found"
    end

    def values_at(*args)
      ret = []
      args.each do |key|
        if self.contains_key?(key)
          ret << self.get(key)
        else
          ret << nil
        end
      end
      ret
    end
    alias :indexes :values_at
    alias :indices :values_at

    def shift
      if self.size == 0
        nil
      else
        k = self.keys.first
        [k, self.remove_field(k)]
      end
    end

    def []=(key,val)
      k = key.kind_of?(String) ? key : key.to_s
      self.put(k, val)
    end

    def store(key,val)
      k = key.kind_of?(String) ? key : key.to_s
      self.put(k.dup.freeze, val)
    end

    def key?(key)
      self.contains_key?(key)
    end
    alias :has_key? :key?

    def value?(val)
      self.contains_value?(val)
    end
    alias :has_value? :value?

    def inspect
      self.to_hash.inspect
    end
    alias :to_s :inspect

    def delete(key)
      unless self.contains_key?(key)
        block_given? ? yield(key)  : nil
      else
        self.remove_field(key)
      end
    end

    def delete_if(&block)
      self.each do |k,v|
        if yield(k, v)
          delete(k)
        end
      end
    end