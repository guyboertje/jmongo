module JMongo
  java_import com.mongodb.BasicDBList
  java_import com.mongodb.BasicDBObject
  java_import com.mongodb.Bytes
  java_import com.mongodb.DB
  java_import com.mongodb.DBCollection
  java_import com.mongodb.DBCursor
  java_import com.mongodb.DBObject
  java_import com.mongodb.Mongo
  java_import com.mongodb.MongoOptions
  java_import com.mongodb.ServerAddress
  java_import com.mongodb.WriteConcern
  java_import com.mongodb.WriteResult
  java_import com.mongodb.MongoException
  java_import com.mongodb.MongoURI
  java_import com.mongodb.MapReduceCommand
  java_import com.mongodb.MapReduceOutput

  SECS_TO_MILLI_FUNC = lambda { |v| v.to_f * 1_000 }  
  DO_NOTHING_FUNC = lambda { |v| v }

  RUBY_JAVA_OPTIONS_KEYS = {
    :pool_size => [:connections_per_host, DO_NOTHING_FUNC],
    :pool_timeout => [:max_wait_time, SECS_TO_MILLI_FUNC],
    :timeout => [:max_wait_time, SECS_TO_MILLI_FUNC],
    :op_timeout => [:socket_timeout, SECS_TO_MILLI_FUNC],
    :connect_timeout => [:connect_timeout, SECS_TO_MILLI_FUNC]
  }

  RUBY_JAVA_OPTIONS_KEYS.default = [nil, DO_NOTHING_FUNC]

  def self.options_ruby2java_lu(key)
    RUBY_JAVA_OPTIONS_KEYS[key].first || key
  end

  def self.options_ruby2java_xf(key,val)
    RUBY_JAVA_OPTIONS_KEYS[key].last[val] || val
  end
end

class Java::ComMongodb::BasicDBObject
  def self.[](*args)
    Hash[*args]
  end

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
