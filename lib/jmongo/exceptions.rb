module Mongo
#  class MongoRubyError < StandardError; end
#  class MongoDBError < RuntimeError; end
#  class ConnectionError < MongoRubyError; end
#  class OperationFailure < MongoDBError; end

  class MongoDBError  < Java::ComMongodb::MongoException; end
  class ConnectionError < Java::ComMongodb::MongoException::Network; end
  class OperationFailure < Java::ComMongodb::MongoException::DuplicateKey; end
end
