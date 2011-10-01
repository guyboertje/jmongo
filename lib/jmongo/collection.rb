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

  class Collection
    include Mongo::JavaImpl::Utils
    include Mongo::JavaImpl::Collection_

    attr_reader :j_collection, :j_db
    attr_reader :db, :name, :pk_factory, :hint

    # Initialize a collection object.
    #
    # @param [DB] db a MongoDB database instance.
    # @param [String, Symbol] name the name of the collection.
    #
    # @raise [InvalidNSName]
    #   if collection name is empty, contains '$', or starts or ends with '.'
    #
    # @raise [TypeError]
    #   if collection name is not a string or symbol
    #
    # @return [Collection]
    #
    # @core collections constructor_details
    #db, name, pk_factory=nil, j_collection=nil
    def initialize(*args)
      j_collection = nil
      @opts = {}
      if args.size == 4
        j_collection = args.pop
      end
      if args.size == 3
        @opts = args.pop
      end
      if args.size < 2
        raise ArgumentError.new("Must supply at least name and db parameters")
      end
      if args.first.respond_to?('collection_names')
        db, name = args
      else
        name, db = args
      end

      @name = validate_name(name)
      @db, @j_db  = db, db.j_db
      @connection = @db.connection
      @pk_factory = @opts[:pk] || BSON::ObjectId
      @hint = nil
      @j_collection = j_collection || @j_db.get_collection(@name)
    end

    # Return a sub-collection of this collection by name. If 'users' is a collection, then
    # 'users.comments' is a sub-collection of users.
    #
    # @param [String] name
    #   the collection to return
    #
    # @raise [Mongo::InvalidNSName]
    #   if passed an invalid collection name
    #
    # @return [Collection]
    #   the specified sub-collection
    def [](name)
      new_name = "#{self.name}.#{name}"
      validate_name new_name
      @db.create_collection(new_name, @opts)
    end

    def capped?
      @j_collection.isCapped
    end
    # Set a hint field for query optimizer. Hint may be a single field
    # name, array of field names, or a hash (preferably an [OrderedHash]).
    # If using MongoDB > 1.1, you probably don't ever need to set a hint.
    #
    # @param [String, Array, OrderedHash] hint a single field, an array of
    #   fields, or a hash specifying fields
    def hint=(hint=nil)
    end

    # Query the database.
    #
    # The +selector+ argument is a prototype document that all results must
    # match. For example:
    #
    #   collection.find({"hello" => "world"})
    #
    # only matches documents that have a key "hello" with value "world".
    # Matches can have other keys *in addition* to "hello".
    #
    # If given an optional block +find+ will yield a Cursor to that block,
    # close the cursor, and then return nil. This guarantees that partially
    # evaluated cursors will be closed. If given no block +find+ returns a
    # cursor.
    #
    # @param [Hash] selector
    #   a document specifying elements which must be present for a
    #   document to be included in the result set.
    #
    # @option opts [Array, Hash] :fields field names that should be returned in the result
    #   set ("_id" will always be included). By limiting results to a certain subset of fields,
    #   you can cut down on network traffic and decoding time. If using a Hash, keys should be field
    #   names and values should be either 1 or 0, depending on whether you want to include or exclude
    #   the given field.
    # @option opts [Integer] :skip number of documents to skip from the beginning of the result set
    # @option opts [Integer] :limit maximum number of documents to return
    # @option opts [Array]   :sort an array of [key, direction] pairs to sort by. Direction should
    #   be specified as Mongo::ASCENDING (or :ascending / :asc) or Mongo::DESCENDING (or :descending / :desc)
    # @option opts [String, Array, OrderedHash] :hint hint for query optimizer, usually not necessary if using MongoDB > 1.1
    # @option opts [Boolean] :snapshot ('false') if true, snapshot mode will be used for this query.
    #   Snapshot mode assures no duplicates are returned, or objects missed, which were preset at both the start and
    #   end of the query's execution. For details see http://www.mongodb.org/display/DOCS/How+to+do+Snapshotting+in+the+Mongo+Database
    # @option opts [Boolean] :batch_size (100) the number of documents to returned by the database per GETMORE operation. A value of 0
    #   will let the database server decide how many results to returns. This option can be ignored for most use cases.
    # @option opts [Boolean] :timeout ('true') when +true+, the returned cursor will be subject to
    #   the normal cursor timeout behavior of the mongod process. When +false+, the returned cursor will never timeout. Note
    #   that disabling timeout will only work when #find is invoked with a block. This is to prevent any inadvertant failure to
    #   close the cursor, as the cursor is explicitly closed when block code finishes.
    #
    # @raise [ArgumentError]
    #   if timeout is set to false and find is not invoked in a block
    #
    # @raise [RuntimeError]
    #   if given unknown options
    #
    # @core find find-instance_method
    def find(selector={}, opts={})
      fields = opts.delete(:fields)
      fields = ["_id"] if fields && fields.empty?
      skip   = opts.delete(:skip) || skip || 0
      limit  = opts.delete(:limit) || 0
      sort   = opts.delete(:sort)
      hint   = opts.delete(:hint)
      snapshot = opts.delete(:snapshot)
      batch_size = opts.delete(:batch_size)
      timeout    = (opts.delete(:timeout) == false) ? false : true
      transformer = opts.delete(:transformer)
      if timeout == false && !block_given?
        raise ArgumentError, "Timeout can be set to false only when #find is invoked with a block."
      end

      if hint
        hint = normalize_hint_fields(hint)
      else
        hint = @hint        # assumed to be normalized already
      end

      raise RuntimeError, "Unknown options [#{opts.inspect}]" unless opts.empty?

      cursor = Cursor.new(self, :selector => selector, :fields => fields, :skip => skip, :limit => limit,
                                :order => sort, :hint => hint, :snapshot => snapshot,
                                :batch_size => batch_size, :timeout => timeout)#,
                                #:transformer => transformer)
      if block_given?
        yield cursor
        cursor.close
        nil
      else
        cursor
      end
    end

    # Return a single object from the database.
    #
    # @return [OrderedHash, Nil]
    #   a single document or nil if no result is found.
    #
    # @param [Hash, ObjectID, Nil] spec_or_object_id a hash specifying elements
    #   which must be present for a document to be included in the result set or an
    #   instance of ObjectID to be used as the value for an _id query.
    #   If nil, an empty selector, {}, will be used.
    #
    # @option opts [Hash]
    #   any valid options that can be send to Collection#find
    #
    # @raise [TypeError]
    #   if the argument is of an improper type.
    def find_one(spec_or_object_id=nil, opts={})
      spec = case spec_or_object_id
             when nil
               {}
             when BSON::ObjectId
               {'_id' => spec_or_object_id}
             when Hash
               spec_or_object_id
             else
               raise TypeError, "spec_or_object_id must be an instance of ObjectId or Hash, or nil"
             end
      fields = opts.fetch(:fields, opts.fetch('fields', {}))
      begin
        find_one_document(spec, fields)
      rescue => ex
        raise OperationFailure, ex.message
      end
    end

    # Save a document to this collection.
    #
    # @param [Hash] doc
    #   the document to be saved. If the document already has an '_id' key,
    #   then an update (upsert) operation will be performed, and any existing
    #   document with that _id is overwritten. Otherwise an insert operation is performed.
    #
    # @return [ObjectID] the _id of the saved document.
    #
    # @option opts [Boolean] :safe (+false+)
    #   If true, check that the save succeeded. OperationFailure
    #   will be raised on an error. Note that a safe check requires an extra
    #   round-trip to the database.
    def save(doc, options={})
      save_document(doc, (options[:safe] || false))
    end

    # Insert one or more documents into the collection.
    #
    # @param [Hash, Array] doc_or_docs
    #   a document (as a hash) or array of documents to be inserted.
    #
    # @return [ObjectID, Array]
    #   the _id of the inserted document or a list of _ids of all inserted documents.
    #   Note: the object may have been modified by the database's PK factory, if it has one.
    #
    # @option opts [Boolean] :safe (+false+)
    #   If true, check that the save succeeded. OperationFailure
    #   will be raised on an error. Note that a safe check requires an extra
    #   round-trip to the database.
    #
    # @core insert insert-instance_method
    def insert(doc_or_docs, options={})
      doc_or_docs = [doc_or_docs] unless doc_or_docs.kind_of?(Array)
      doc_or_docs.collect! { |doc| @pk_factory.create_pk(doc) }
      safe = (options[:safe] || false)
      continue = (options[:continue_on_error] || false)
      docs = insert_documents(doc_or_docs, safe, continue)
      docs.size == 1 ? docs.first['_id'] : docs.collect{|doc| doc['_id']}
    end
    alias_method :<<, :insert

    # Remove all documents from this collection.
    #
    # @param [Hash] selector
    #   If specified, only matching documents will be removed.
    #
    # @option opts [Boolean] :safe [false] run the operation in safe mode, which
    #   will call :getlasterror on the database and report any assertions.
    #
    # @example remove all documents from the 'users' collection:
    #   users.remove
    #   users.remove({})
    #
    # @example remove only documents that have expired:
    #   users.remove({:expire => {"$lte" => Time.now}})
    #
    # @return [True]
    #
    # @raise [Mongo::OperationFailure] an exception will be raised iff safe mode is enabled
    #   and the operation fails.
    #
    # @core remove remove-instance_method
    def remove(selector={}, options={})
      safe = (options[:safe] || false)
      remove_documents(selector,safe)
    end

    # Update a single document in this collection.
    #
    # @param [Hash] selector
    #   a hash specifying elements which must be present for a document to be updated. Note:
    #   the update command currently updates only the first document matching the
    #   given selector. If you want all matching documents to be updated, be sure
    #   to specify :multi => true.
    # @param [Hash] document
    #   a hash specifying the fields to be changed in the selected document,
    #   or (in the case of an upsert) the document to be inserted
    #
    # @option [Boolean] :upsert (+false+) if true, performs an upsert (update or insert)
    # @option [Boolean] :multi (+false+) update all documents matching the selector, as opposed to
    #   just the first matching document. Note: only works in MongoDB 1.1.3 or later.
    # @option opts [Boolean] :safe (+false+)
    #   If true, check that the save succeeded. OperationFailure
    #   will be raised on an error. Note that a safe check requires an extra
    #   round-trip to the database.  NOTE!!!! Java driver does not have a safe option for update
    #
    # @core update update-instance_method
    def update(selector, document, options={})
      upsert, multi = !!(options[:upsert]), !!(options[:multi])
      safe = (options[:safe] || false)
      update_documents(selector, document, upsert, multi, safe)
    end

    # Create a new index.
    #
    # @param [String, Array] spec
    #   should be either a single field name or an array of
    #   [field name, direction] pairs. Directions should be specified
    #   as Mongo::ASCENDING, Mongo::DESCENDING, or Mongo::GEO2D.
    #
    #   Note that geospatial indexing only works with versions of MongoDB >= 1.3.3+. Keep in mind, too,
    #   that in order to geo-index a given field, that field must reference either an array or a sub-object
    #   where the first two values represent x- and y-coordinates. Examples can be seen below.
    #
    #   Also note that it is permissible to create compound indexes that include a geospatial index as
    #   long as the geospatial index comes first.
    #
    # @param [Boolean] unique if true, this index will enforce a uniqueness constraint. DEPRECATED. Future
    #   versions of this driver will specify the uniqueness constraint using a hash param.
    #
    # @option opts [Boolean] :unique (false) if true, this index will enforce a uniqueness constraint.
    # @option opts [Boolean] :background (false) indicate that the index should be built in the background. This
    #   feature is only available in MongoDB >= 1.3.2.
    # @option opts [Boolean] :dropDups If creating a unique index on a collection with pre-existing records,
    #   this option will keep the first document the database indexes and drop all subsequent with duplicate values.
    # @option opts [Integer] :min specify the minimum longitude and latitude for a geo index.
    # @option opts [Integer] :max specify the maximum longitude and latitude for a geo index.
    #
    # @example Creating a compound index:
    #   @posts.create_index([['subject', Mongo::ASCENDING], ['created_at', Mongo::DESCENDING]])
    #
    # @example Creating a geospatial index:
    #   @restaurants.create_index([['location', Mongo::GEO2D]])
    #
    #   # Note that this will work only if 'location' represents x,y coordinates:
    #   {'location': [0, 50]}
    #   {'location': {'x' => 0, 'y' => 50}}
    #   {'location': {'latitude' => 0, 'longitude' => 50}}
    #
    # @example A geospatial index with alternate longitude and latitude:
    #   @restaurants.create_index([['location', Mongo::GEO2D]], :min => 500, :max => 500)
    #
    # @return [String] the name of the index created.
    #
    # @core indexes create_index-instance_method
    def create_index(spec, opts={})
      _create_indexes(spec,opts)
    end
    alias_method :ensure_index, :create_index

    # Drop a specified index.
    #
    # @param [String] name
    #
    # @core indexes
    def drop_index(name)
      raise MongoArgumentError, "Cannot drop index for nil name" unless name
      _drop_index(name)
    end

    # Drop all indexes.
    #
    # @core indexes
    def drop_indexes
      # Note: calling drop_indexes with no args will drop them all.
      @j_collection.dropIndexes('*')
    end

    # Drop the entire collection. USE WITH CAUTION.
    def drop
      @j_collection.drop
    end


    # Atomically update and return a document using MongoDB's findAndModify command. (MongoDB > 1.3.0)
    #
    # @option opts [Hash] :update (nil) the update operation to perform on the matched document.
    # @option opts [Hash] :query ({}) a query selector document for matching the desired document.
    # @option opts [Array, String, OrderedHash] :sort ({}) specify a sort option for the query using any
    #   of the sort options available for Cursor#sort. Sort order is important if the query will be matching
    #   multiple documents since only the first matching document will be updated and returned.
    # @option opts [Boolean] :remove (false) If true, removes the the returned document from the collection.
    # @option opts [Boolean] :new (false) If true, returns the updated document; otherwise, returns the document
    #   prior to update.
    #
    # @return [Hash] the matched document.
    #
    # @core findandmodify find_and_modify-instance_method
    def find_and_modify(opts={})
      query  = opts[:query] || {}
      fields = opts[:fields] || {}
      sort   = prep_sort(opts[:sort] || [])
      update = opts[:update] || {}
      remove = opts[:remove] || false
      new_    = opts[:new] || false
      upsert = opts[:upsert] || false
      trap_raise(OperationFailure) do
        find_and_modify_document(query, fields, sort, remove, update, new_, upsert)
      end
    end

    # Perform a map/reduce operation on the current collection.
    #
    # @param [String, BSON::Code] map a map function, written in JavaScript.
    # @param [String, BSON::Code] reduce a reduce function, written in JavaScript.
    #
    # @option opts [Hash] :query ({}) a query selector document, like what's passed to #find, to limit
    #   the operation to a subset of the collection.
    # @option opts [Array] :sort ([]) an array of [key, direction] pairs to sort by. Direction should
    #   be specified as Mongo::ASCENDING (or :ascending / :asc) or Mongo::DESCENDING (or :descending / :desc)
    # @option opts [Integer] :limit (nil) if passing a query, number of objects to return from the collection.
    # @option opts [String, BSON::Code] :finalize (nil) a javascript function to apply to the result set after the
    #   map/reduce operation has finished.
    # @option opts [String] :out (nil) the name of the output collection. If specified, the collection will not be treated as temporary.
    # @option opts [Boolean] :keeptemp (false) if true, the generated collection will be persisted. default is false.
    # @option opts [Boolean ] :verbose (false) if true, provides statistics on job execution time.
    #
    # @return [Collection] a collection containing the results of the operation.
    #
    # @see http://www.mongodb.org/display/DOCS/MapReduce Offical MongoDB map/reduce documentation.
    #
    # @core mapreduce map_reduce-instance_method
    def map_reduce(map, reduce, opts={})
      query = opts.fetch(:query,{})
      sort = opts.fetch(:sort,[])
      limit = opts.fetch(:limit,0)
      finalize = opts[:finalize]
      out = opts[:out]
      keeptemp = opts.fetch(:keeptemp,true)
      verbose = opts.fetch(:verbose,true)
      raw     = opts.delete(:raw)

      m = map.to_s
      r = reduce.to_s

      mrc = case out
          when String
            JMongo::MapReduceCommand.new(@j_collection, m, r, out, REPLACE, to_dbobject(query))
          when Hash
            if out.keys.size != 1
              raise ArgumentError, "You need to specify one key value pair in the out hash"
            end
            out_type = out.keys.first
            out_val = out[out_type]
            unless MapReduceEnumHash.keys.include?(out_type)
              raise ArgumentError, "Your out hash must have one of these keys: #{MapReduceEnumHash.keys}"
            end
            out_type_enum = MapReduceEnumHash[out_type]
            out_dest = out_val.is_a?(String) ? out_val : nil
            JMongo::MapReduceCommand.new(@j_collection, m, r, out_dest, out_type_enum, to_dbobject(query))
          else
            raise ArgumentError, "You need to specify an out parameter in the options hash"
          end

      mrc.verbose = verbose
      mrc.sort = prep_sort(sort)
      mrc.limit = limit
      mrc.finalize = finalize
      result =  from_dbobject(@j_db.command(mrc.toDBObject))

      if raw
        result
      elsif result["result"]
        @db[result["result"]]
      else
        raise ArgumentError, "Could not instantiate collection from result. If you specified " +
          "{:out => {:inline => true}}, then you must also specify :raw => true to get the results."
      end
    end
    alias :mapreduce :map_reduce

    # Perform a group aggregation.
    #
    # @param [Hash] opts the options for this group operation. The minimum required are :initial
    #   and :reduce.
    #
    # @option opts [Array, String, Symbol] :key (nil) Either the name of a field or a list of fields to group by (optional).
    # @option opts [String, BSON::Code] :keyf (nil) A JavaScript function to be used to generate the grouping keys (optional).
    # @option opts [String, BSON::Code] :cond ({}) A document specifying a query for filtering the documents over
    #   which the aggregation is run (optional).
    # @option opts [Hash] :initial the initial value of the aggregation counter object (required).
    # @option opts [String, BSON::Code] :reduce (nil) a JavaScript aggregation function (required).
    # @option opts [String, BSON::Code] :finalize (nil) a JavaScript function that receives and modifies
    #   each of the resultant grouped objects. Available only when group is run with command
    #   set to true.
    #
    # @return [Array] the command response consisting of grouped items.
    def group(opts, condition={}, initial={}, reduce=nil, finalize=nil)
      if opts.is_a?(Hash)
        return __group(opts)
      else
        warn "Collection#group no longer take a list of parameters. This usage is deprecated and will be remove in v2.0." +
             "Check out the new API at http://api.mongodb.org/ruby/current/Mongo/Collection.html#group-instance_method"
      end

      reduce = BSON::Code.new(reduce) unless reduce.is_a?(BSON::Code)

      group_command = {
        "group" => {
          "ns"      => @name,
          "$reduce" => reduce,
          "cond"    => condition,
          "initial" => initial
        }
      }

      if opts.is_a?(Symbol)
        raise MongoArgumentError, "Group takes either an array of fields to group by or a JavaScript function" +
          "in the form of a String or BSON::Code."
      end

      unless opts.nil?
        if opts.is_a? Array
          key_type = "key"
          key_value = {}
          opts.each { |k| key_value[k] = 1 }
        else
          key_type  = "$keyf"
          key_value = opts.is_a?(BSON::Code) ? opts : BSON::Code.new(opts)
        end

        group_command["group"][key_type] = key_value
      end

      finalize = BSON::Code.new(finalize) if finalize.is_a?(String)
      if finalize.is_a?(BSON::Code)
        group_command['group']['finalize'] = finalize
      end

      result = @db.command(group_command)

      if Mongo::Support.ok?(result)
        result["retval"]
      else
        raise OperationFailure, "group command failed: #{result['errmsg']}"
      end
    end

    def __group(opts={})
      reduce   =  opts[:reduce]
      finalize =  opts[:finalize]
      cond     =  opts.fetch(:cond, {})
      initial  =  opts[:initial]

      if !(reduce && initial)
        raise MongoArgumentError, "Group requires at minimum values for initial and reduce."
      end

      cmd = {
        "group" => {
          "ns"      => @name,
          "$reduce" => reduce.to_bson_code,
          "cond"    => cond,
          "initial" => initial
        }
      }

      if finalize
        cmd['group']['finalize'] = finalize.to_bson_code
      end

      if key = opts[:key]
        if key.is_a?(String) || key.is_a?(Symbol)
          key = [key]
        end
        key_value = {}
        key.each { |k| key_value[k] = 1 }
        cmd["group"]["key"] = key_value
      elsif keyf = opts[:keyf]
        cmd["group"]["$keyf"] = keyf.to_bson_code
      end

      result = @db.command(cmd)
      result["retval"]
    end

    private :__group

    # Return a list of distinct values for +key+ across all
    # documents in the collection. The key may use dot notation
    # to reach into an embedded object.
    #
    # @param [String, Symbol, OrderedHash] key or hash to group by.
    # @param [Hash] query a selector for limiting the result set over which to group.
    #
    # @example Saving zip codes and ages and returning distinct results.
    #   @collection.save({:zip => 10010, :name => {:age => 27}})
    #   @collection.save({:zip => 94108, :name => {:age => 24}})
    #   @collection.save({:zip => 10010, :name => {:age => 27}})
    #   @collection.save({:zip => 99701, :name => {:age => 24}})
    #   @collection.save({:zip => 94108, :name => {:age => 27}})
    #
    #   @collection.distinct(:zip)
    #     [10010, 94108, 99701]
    #   @collection.distinct("name.age")
    #     [27, 24]
    #
    #   # You may also pass a document selector as the second parameter
    #   # to limit the documents over which distinct is run:
    #   @collection.distinct("name.age", {"name.age" => {"$gt" => 24}})
    #     [27]
    #
    # @return [Array] an array of distinct values.
    def distinct(key, query=nil)
      raise MongoArgumentError unless [String, Symbol].include?(key.class)
      if query
        from_dbobject @j_collection.distinct(key.to_s, to_dbobject(query))
      else
        from_dbobject @j_collection.distinct(key.to_s)
      end
    end

    # Rename this collection.
    #
    # Note: If operating in auth mode, the client must be authorized as an admin to
    # perform this operation.
    #
    # @param [String] new_name the new name for this collection
    #
    # @raise [Mongo::InvalidNSName] if +new_name+ is an invalid collection name.
    def rename(new_name)
      name = validate_name(new_name)
      begin
        jcol = @j_collection.rename(name)
        @name = name
        @j_collection = jcol
      rescue => ex
        raise MongoDBError, "Error renaming collection: #{name}, more: #{ex.message}"
      end
    end

    # Get information on the indexes for this collection.
    #
    # @return [Hash] a hash where the keys are index names.
    #
    # @core indexes
    def index_information
      @db.index_information(@name)
    end

    # Return a hash containing options that apply to this collection.
    # For all possible keys and values, see DB#create_collection.
    #
    # @return [Hash] options that apply to this collection.
    def options
      info = @db.collections_info(@name).to_a
      ap info
      info.last['options']
    end

    # Return stats on the collection. Uses MongoDB's collstats command.
    #
    # @return [Hash]
    def stats
      @db.command({:collstats => @name})
    end

    # Get the number of documents in this collection.
    #
    # @return [Integer]
    def count(opts={})
      return @j_collection.count() if opts.empty?
      query = opts[:query] || opts['query'] || {}
      fields = opts[:fields] || opts['fields'] || {}
      limit = opts[:limit] || opts['limit'] || 0
      skip = opts[:skip] || opts['skip'] || 0
      @j_collection.get_count(to_dbobject(query), to_dbobject(fields), limit, skip)
    end

    alias :size :count

    protected

    def validate_name(new_name)
      raise TypeError, "new_name must be a string like" unless new_name.respond_to?(:to_s)

      name = new_name.to_s

      if name.empty? || name.include?("..")
        raise Mongo::InvalidNSName, "collection names cannot be empty"
      end
      if name.include? "$"
        raise Mongo::InvalidNSName, "collection names must not contain '$'" unless name =~ /((^\$cmd)|(oplog\.\$main))/
      end
      if name.match(/^\./) || name.match(/\.$/)
        raise Mongo::InvalidNSName, "collection names must not start or end with '.'"
      end
      name
    end

    def normalize_hint_fields(hint)
      case hint
      when String
        {hint => 1}
      when Hash
        hint
      when nil
        nil
      else
        h = BSON::OrderedHash.new
        hint.to_a.each { |k| h[k.to_s] = 1 }
        h
      end
    end
  end
end
