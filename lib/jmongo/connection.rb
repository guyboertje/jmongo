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

    attr_reader :connection, :connector, :logger, :auths, :primary, :write_concern

    DEFAULT_PORT = 27017

    def initialize host = nil, port = nil, opts = {}
      @logger = opts.delete(:logger)
      @auths = opts.delete(:auths) || []
      if opts.has_key?(:new_from_uri)
        @mongo_uri = opts[:new_from_uri]
        @options = @mongo_uri.options
        @write_concern = @options.write_concern
        @connection = JMongo::MongoClient.new(@mongo_uri)
      else
        @host = host || 'localhost'
        @port = port || 27017
        @server_address = JMongo::ServerAddress.new @host, @port
        @options = JMongo::MongoClientOptions::Builder.new
        opts.each do |k,v|
          key = k.to_sym
          jmo_key = JMongo.options_ruby2java_lu(key)
          case jmo_key
          when :safe
            @write_concern = DB.write_concern(v)
            @options.write_concern @write_concern
          else
            jmo_val = JMongo.options_ruby2java_xf(key, v)
            @options.send("#{jmo_key}=", jmo_val)
          end
        end
        @connection = JMongo::MongoClient.new(@server_address, @options.build)
      end
      @connector = @connection.connector
      add = @connector.address
      @primary = [add.host, add.port]
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
      return false if @auths.empty?
      @auths.each do |auth|
        self[auth['db_name']].authenticate(auth['username'], auth['password'], false)
      end
      true
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
      remove_auth(db_name)
      auth = {}
      auth['db_name']  = db_name
      auth['username'] = username
      auth['password'] = password
      @auths << auth
      auth
    end

    # Remove a saved authentication for this connection.
    #
    # @param [String] db_name
    #
    # @return [Boolean]
    def remove_auth(db_name)
      return unless @auths
      if @auths.reject! { |a| a['db_name'] == db_name }
        true
      else
        false
      end
    end

    # Remove all authenication information stored in this connection.
    #
    # @return [true] this operation return true because it always succeeds.
    def clear_auths
      @auths = []
      true
    end

    # Return a hash with all database names
    # and their respective sizes on disk.
    #
    # @return [Hash]
    def database_info
      doc = self['admin'].command({:listDatabases => 1})
      doc['databases'].each_with_object({}) do |db, info|
        info[db['name']] = db['sizeOnDisk'].to_i
      end
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
      db db_name
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

    # Checks if a server is alive. This command will return immediately
    # even if the server is in a lock.
    #
    # @return [Hash]
    def ping
      db("admin").command('ping')
    end
    # Get the build information for the current connection.
    #
    # @return [Hash]
    def server_info
      db("admin").command('buildinfo')
    end

    # Get the build version of the current server.
    #
    # @return [Mongo::ServerVersion]
    #   object allowing easy comparability of version.
    def server_version
      ServerVersion.new(server_info["version"])
    end

    # Is it okay to connect to a slave?
    #
    # @return [Boolean]
    def slave_ok?
      raise_not_implemented
    end

    def log_operation(name, payload)
      return unless @logger
      msg = "#{payload[:database]}['#{payload[:collection]}'].#{name}("
      msg += payload.values_at(:selector, :document, :documents, :fields ).compact.map(&:inspect).join(', ') + ")"
      msg += ".skip(#{payload[:skip]})"  if payload[:skip]
      msg += ".limit(#{payload[:limit]})"  if payload[:limit]
      msg += ".sort(#{payload[:order]})"  if payload[:order]
      @logger.debug "MONGODB #{msg}"
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
      connect
    end

    def connected?
      @connection && @connector && @connector.is_open
    end

    def connect
      close
      if @mongo_uri
        @connection = JMongo::Mongo.new(@mongo_uri)
      else
        @connection = JMongo::Mongo.new(@server_address, @options)
      end
      @connector = @connection.connector
    end
    alias :reconnect :connect

    def close
      @connection.close if @connection
      @connection = @connector = nil
    end

  end # class Connection

end # module Mongo
