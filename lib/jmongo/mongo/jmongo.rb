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
  import com.mongodb.MapReduceCommand
  import com.mongodb.MapReduceOutput
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
