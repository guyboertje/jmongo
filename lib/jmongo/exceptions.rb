module Mongo
  MongoDBException = Java::ComMongodb::MongoException
  NetworkException = Java::ComMongodb::MongoException::Network
  DuplicateKeyException = Java::ComMongodb::MongoException::DuplicateKey

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
