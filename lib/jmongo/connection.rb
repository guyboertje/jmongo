# Copyright (C) 2008-2010 10gen Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo

  class Connection
    include Mongo::JavaImpl::Utils
     extend Mongo::JavaImpl::NoImplYetClass
    include Mongo::JavaImpl::Connection_::InstanceMethods
     extend Mongo::JavaImpl::Connection_::ClassMethods

    attr_reader :connection

    def initialize host = nil, port = nil, opts = {}
      if opts.has_key?(:new_from_uri)
        @connection = opts[:new_from_uri]
      else
        @host = host || 'localhost'
        @port = port || 27017
        server_address = JMongo::ServerAddress.new @host, @port
        options = JMongo::MongoOptions.new
        options.connectionsPerHost = opts[:pool_size] || 1
        options.socketTimeout = opts[:timeout].to_i * 1000 || 5000
        @connection = JMongo::Mongo.new(server_address, options)
      end
    end

    def self.paired(nodes, opts={})
      raise_not_implemented
    end

    # Initialize a connection to MongoDB using the MongoDB URI spec:
    #
    # @param uri [String]
    #   A string of the format mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/database]
    #
    # @param opts Any of the options available for Connection.new
    #
    # @return [Mongo::Connection]
    def self.from_uri(uri, opts={})
      _from_uri(uri,opts)
    end

    # Apply each of the saved database authentications.
    #
    # @return [Boolean] returns true if authentications exist and succeeed, false
    #   if none exists.
    #
    # @raise [AuthenticationError] raises an exception if any one
    #   authentication fails.
    def apply_saved_authentication
      raise_not_implemented
    end

    # Save an authentication to this connection. When connecting,
    # the connection will attempt to re-authenticate on every db
    # specificed in the list of auths. This method is called automatically
    # by DB#authenticate.
    #
    # @param [String] db_name
    # @param [String] username
    # @param [String] password
    #
    # @return [Hash] a hash representing the authentication just added.
    def add_auth(db_name, username, password)
      raise_not_implemented
    end

    # Remove a saved authentication for this connection.
    #
    # @param [String] db_name
    #
    # @return [Boolean]
    def remove_auth(db_name)
      raise_not_implemented
    end

    # Remove all authenication information stored in this connection.
    #
    # @return [true] this operation return true because it always succeeds.
    def clear_auths
      raise_not_implemented
    end

    # Return a hash with all database names
    # and their respective sizes on disk.
    #
    # @return [Hash]
    def database_info
      raise_not_implemented
    end

    # Return an array of database names.
    #
    # @return [Array]
    def database_names
      get_db_names
    end

    # Return a database with the given name.
    # See DB#new for valid options hash parameters.
    #
    # @param [String] db_name a valid database name.
    #
    # @return [Mongo::DB]
    #
    # @core databases db-instance_method
    def db(db_name, options={})
      DB.new db_name, self, options
    end

    # Shortcut for returning a database. Use DB#db to accept options.
    #
    # @param [String] db_name a valid database name.
    #
    # @return [Mongo::DB]
    #
    # @core databases []-instance_method
    def [](db_name)
      DB.new db_name, self
    end

    # Drop a database.
    #
    # @param [String] name name of an existing database.
    def drop_database(name)
      drop_a_db name
    end

    # Copy the database +from+ to +to+ on localhost. The +from+ database is
    # assumed to be on localhost, but an alternate host can be specified.
    #
    # @param [String] from name of the database to copy from.
    # @param [String] to name of the database to copy to.
    # @param [String] from_host host of the 'from' database.
    # @param [String] username username for authentication against from_db (>=1.3.x).
    # @param [String] password password for authentication against from_db (>=1.3.x).
    def copy_database(from, to, from_host="localhost", username=nil, password=nil)
      raise_not_implemented
    end

    # Increment and return the next available request id.
    #
    # return [Integer]
    def get_request_id
      raise_not_implemented
    end

    # Get the build information for the current connection.
    #
    # @return [Hash]
    def server_info
      raise_not_implemented
    end

    # Get the build version of the current server.
    #
    # @return [Mongo::ServerVersion]
    #   object allowing easy comparability of version.
    def server_version
      _server_version
    end

    # Is it okay to connect to a slave?
    #
    # @return [Boolean]
    def slave_ok?
      raise_not_implemented
    end


    ## Connections and pooling ##

    # Send a message to MongoDB, adding the necessary headers.
    #
    # @param [Integer] operation a MongoDB opcode.
    # @param [BSON::ByteBuffer] message a message to send to the database.
    # @param [String] log_message text version of +message+ for logging.
    #
    # @return [True]
    def send_message(operation, message, log_message=nil)
      raise_not_implemented
    end

    def send_message_with_safe_check(operation, message, db_name, log_message=nil)
      raise_not_implemented
    end

    def receive_message(operation, message, log_message=nil, socket=nil)
      raise_not_implemented
    end

    def connect_to_master
      raise_not_implemented
    end

    def connected?
      raise_not_implemented
    end

    def close
      raise_not_implemented
    end

  end # class Connection

end # module Mongo
