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

  class DB
    include Mongo::JavaImpl::Db_
    include Mongo::JavaImpl::Utils

    attr_reader :j_db
    attr_reader :name
    attr_reader :connection

    attr_writer :strict

    ProfileLevel = {:off => 0, :slow_only => 1, :all => 2, 0 => 'off', 1 => 'slow_only', 2 => 'all'}

    def initialize(db_name, connection, options={})
      @name       = db_name
      @connection = connection
      @j_db = @connection.connection.get_db db_name
      @pk_factory = options[:pk]
    end

    def authenticate(username, password, save_auth=true)
      begin
        succeeded = @j_db.authenticate(username, password)
        if save_auth && succeeded
          @connection.add_auth(@name, username, password)
        end
      rescue => e
        succeeded = false
      end
      succeeded
    end

    def add_user(username, password)
      @j_db.add_user(username, password)
    end

    def remove_user(username)
      @j_db.remove_user(username)
    end

    def logout
      raise_not_implemented
    end

    def collection_names
      @j_db.get_collection_names
    end

    def collections
      collection_names.map do |name|
        Collection.new(self, name)
      end
    end

    def collections_info(coll_name=nil)
      selector = {}
      selector[:name] = full_collection_name(coll_name) if coll_name
      coll = self.collection(SYSTEM_NAMESPACE_COLLECTION)
      coll.find :selector => selector
    end

    def create_collection(name, options={})
      begin
        jc = @j_db.create_collection(name, to_dbobject(options))
        Collection.new self, name, nil, jc
      rescue NativeException => ex
        raise MongoDBError, "Collection #{name} already exists. " +
            "Currently in strict mode."
      end
    end

    def collection(name)
      Collection.new self, name
    end
    alias_method :[], :collection

    def drop_collection(name)
      coll = collection(name).j_collection.drop
    end

    def get_last_error
      from_dbobject(@j_db.getLastError)
    end
    alias :last_status :get_last_error

    def error?
      !get_last_error['err'].nil?
    end

    def previous_error
      exec_command :getpreverror
    end

    def reset_error_history
      exec_command :reseterror
    end

    def query(collection, query, admin=false)
      raise_not_implemented
    end

    def dereference(dbref)
      ns = dbref.namespace
      raise MongoArgumentError, "No namespace for dbref: #{dbref.inspect}"
      collection(ns).find_one("_id" => dbref.object_id)
    end

    def eval(code, *args)
      doc = do_eval(code, *args)
      return unless doc
      return doc['retval']['value'] if doc['retval'] && doc['retval']['value']
      doc['retval']
    end

    def rename_collection(from, to)
      oh = BSON::OrderedHash.new
      oh['renameCollection'] = "#{@name}.#{from}"
      oh['to'] = "#{@name}.#{to}"
      doc = DB.new('admin', @connection).command(oh, :check_response => false)
      ok?(doc) || raise(MongoDBError, "Error renaming collection: #{doc.inspect}")
    end

    def drop_index(collection_name, index_name)
      self[collection_name].drop_index(index_name)
    end

    def index_information(collection_name)
      info = {}
      from_dbobject(@j_db.get_collection(collection_name).get_index_info).each do |index|
        info[index['name']] = index
      end
      info
    end

    def stats
      exec_command(:dbstats)
    end

    def create_index(collection_name, field_or_spec, unique=false)
      collection(collection_name).create_indexes(field_or_spec,{:unique=>unique})
    end

    def ok?(doc)
      doc['ok'] == 1.0 || doc['ok'] == true
    end

    def command(selector, opts={})
      check_response = opts.fetch(:check_response, true)
      raise MongoArgumentError, "command must be given a selector" unless selector.is_a?(Hash) && !selector.empty?
      if selector.keys.length > 1 && RUBY_VERSION < '1.9' && selector.class != BSON::OrderedHash
        raise MongoArgumentError, "DB#command requires an OrderedHash when hash contains multiple keys"
      end

      begin
        result = exec_command(selector)
      rescue => ex
        raise OperationFailure, "Database command '#{selector.keys.first}' failed: #{ex.message}"
      end

      raise OperationFailure, "Database command '#{selector.keys.first}' failed: returned null." if result.nil?

      if (check_response && !ok?(result))
        message = "Database command '#{selector.keys.first}' failed: (" + result.map{|k, v| "#{k}: '#{v}'"}.join('; ') + ")."
        raise OperationFailure.new message
      else
        result
      end
    end

    def full_collection_name(collection_name)
      "#{@name}.#{collection_name}"
    end

    # The primary key factory object (or +nil+).
    #
    # @return [Object, Nil]
    def pk_factory
      @pk_factory
    end

    # Specify a primary key factory if not already set.
    #
    # @raise [MongoArgumentError] if the primary key factory has already been set.
    def pk_factory=(pk_factory)
      if @pk_factory
        raise MongoArgumentError, "Cannot change primary key factory once it's been set"
      end

      @pk_factory = pk_factory
    end

    def profiling_level
      oh = BSON::OrderedHash.new
      oh['profile'] = -1
      doc = command(oh, :check_response => false)
      raise "Error with profile command: #{doc.inspect}" unless ok?(doc) && doc['was'].kind_of?(Numeric)
      was = ProfileLevel[doc['was'].to_i]
      raise "Error: illegal profiling level value #{doc['was']}" if was.nil?
      was.to_sym
    end

    def profiling_level=(level)
      oh = BSON::OrderedHash.new
      int_lvl = ProfileLevel[level]
      raise "Error: illegal profiling level value #{level}" if int_lvl.nil?
      oh['profile'] = int_lvl
      doc = command(oh, :check_response => false)
      ok?(doc) || raise(MongoDBError, "Error with profile command: #{doc.inspect}")
    end

    def profiling_info
      Cursor.new(Collection.new(SYSTEM_PROFILE_COLLECTION, self), :selector => {}).to_a
    end

    def validate_collection(name)
      cmd = BSON::OrderedHash.new
      cmd['validate'] = name
      cmd['full'] = true
      doc = command(cmd, :check_response => false)
      if !ok?(doc)
        raise MongoDBError, "Error with validate command: #{doc.inspect}"
      end
      if (doc.has_key?('valid') && !doc['valid']) || (doc['result'] =~ /\b(exception|corrupt)\b/i)
        raise MongoDBError, "Error: invalid collection #{name}: #{doc.inspect}"
      end
      doc
    end

    # additions to the ruby driver
    def has_collection?(name)
      has_coll name
    end
  end # class DB

end # module Mongo
