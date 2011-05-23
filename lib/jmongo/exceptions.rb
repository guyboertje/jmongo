module Mongo
  class MongoDBError  < Java::ComMongodb::MongoException; end
  class ConnectionError < Java::ComMongodb::MongoException::Network; end
end
