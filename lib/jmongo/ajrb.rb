# Copyright (C) 2010 Guy Boertje
#
# Mongo::JavaImpl::Connection_
# Mongo::JavaImpl::Db_
# Mongo::JavaImpl::Collection_
# Mongo::JavaImpl::Utils
#

module Mongo

  module JavaImpl

    module Connection_
      module InstanceMethods
        private
        def get_db_names
          @connection.get_database_names
        end
        def drop_a_db name
          @connection.drop_database(name)
        end
        def _server_version
          @connection.get_version
        end
      end
      module ClassMethods
        URI_RE = /^mongodb:\/\/(([-.\w]+):([^@]+)@)?([-.\w]+)(:([\w]+))?(\/([-\w]+))?/
        OPTS_KEYS = %W[maxpoolsize waitqueuemultiple waitqueuetimeoutms connecttimeoutms sockettimeoutms
                       autoconnectretry slaveok safe w wtimeout fsync]

        def _from_uri uri, opts={}
          optarr = []
          unless uri =~ URI_RE
            raise MongoArgumentError, "MongoDB URI incorrect"
          end
          pieces = uri.split("//")
          extra = pieces.last.count('/') == 0 ? "/" : ""
          opts.each do|k,v|
            if OPTS_KEYS.include?(k.to_s) && !v.nil?
              (optarr << "#{k}=#{v}")
            end
          end
          unless optarr.empty?
            uri << "#{extra}?" << optarr.join("&")
          end
          opts[:new_from_uri] = Java::ComMongodb::MongoURI.new(uri)
          new("",0,opts)
        end
      end
    end

    module Db_
      SYSTEM_NAMESPACE_COLLECTION = "system.namespaces"
      SYSTEM_PROFILE_COLLECTION = "system.profile"

      private

      def exec_command(cmd)
        cmd_hash = cmd.kind_of?(Hash) ? cmd : {cmd => 1}
        cmd_res = @j_db.command(to_dbobject(cmd_hash))
        from_dbobject(cmd_res)
      end

      def do_eval(string, *args)
        @j_db.do_eval(string, *args)
      end

      def has_coll(name)
        @j_db.collection_exists(name)
      end
    end

    module Collection_
      private

      def name_from opts
        return unless (opts[:name] || opts['name'])
        opts.delete(:name) || opts.delete('name')
      end

      def _drop_index(spec)
        name = generate_index_name(parse_index_spec(spec))
        @j_collection.dropIndexes(name)
      end

      def _create_indexes(obj,opts = {})
        name = name_from(opts)
        field_spec = parse_index_spec(obj)
        opts[:dropDups] = opts[:drop_dups] if opts[:drop_dups]
        if obj.is_a?(String) || obj.is_a?(Symbol)
          name = obj.to_s unless name
        end
        name = generate_index_name(field_spec) unless name
        opts['name'] = name
        begin
          @j_collection.ensureIndex(to_dbobject(field_spec),to_dbobject(opts))
        rescue => e
          if opts[:dropDups] && e.message =~ /E11000/
            # NOP. If the user is intentionally dropping dups, we can ignore duplicate key errors.
          else
            msg = "Failed to create index #{field_spec.inspect} with the following error: #{e.message}"
            raise Mongo::OperationFailure, msg
          end
        end
        name
      end

      def generate_index_name(spec)
        return spec.to_s if spec.is_a?(String) || spec.is_a?(Symbol)
        indexes = []
        spec.each_pair do |field, direction|
          dir = sort_value(field,direction)
          indexes.push("#{field}_#{dir}")
        end
        indexes.join("_")
      end

      def parse_index_spec(spec)
        field_spec = Hash.new
        if spec.is_a?(String) || spec.is_a?(Symbol)
          field_spec[spec.to_s] = 1
        elsif spec.is_a?(Array) && spec.all? {|field| field.is_a?(Array) }
          spec.each do |f|
            if [Mongo::ASCENDING, Mongo::DESCENDING, Mongo::GEO2D].include?(f[1])
              field_spec[f[0].to_s] = f[1]
            else
              raise MongoArgumentError, "Invalid index field #{f[1].inspect}; " +
                "should be one of Mongo::ASCENDING (1), Mongo::DESCENDING (-1) or Mongo::GEO2D ('2d')."
            end
          end
        else
          raise MongoArgumentError, "Invalid index specification #{spec.inspect}; " +
            "should be either a string, symbol, or an array of arrays."
        end
        field_spec
      end

      def remove_documents(obj, safe=nil)
        wr = @j_collection.remove(to_dbobject(obj), write_concern(safe))
        wr.get_error.nil? && wr.get_n > 0
      end

      def insert_documents(obj, safe=nil)
        dbo = to_dbobject(obj)
        @j_collection.insert(dbo, write_concern(safe))
        obj.collect { |o| o['_id'] || o[:_id] }
      end

      def find_and_modify_document(query,fields,sort,remove,update,new_,upsert)
        from_dbobject @j_collection.find_and_modify(to_dbobject(query),to_dbobject(fields),to_dbobject(sort),remove,to_dbobject(update),new_,upsert)
      end

      def find_one_document(document, fields)
        from_dbobject @j_collection.findOne(to_dbobject(document),to_dbobject(fields))
      end

      def update_documents(selector, document, upsert=false, multi=false, safe=nil)
        @j_collection.update(to_dbobject(selector),to_dbobject(document), upsert, multi, write_concern(safe))
      end

      def save_document(obj, safe=nil)
        id = obj.delete(:_id) || obj.delete('_id')
        obj['_id'] = id || BSON::ObjectId.new
        db_obj = to_dbobject(obj)
        @j_collection.save(db_obj, write_concern(safe))
        obj['_id']
      end
    end
    module NoImplYetClass
      def raise_not_implemented
        raise NoMethodError, "This method hasn't been implemented yet."
      end
    end
    module Utils
      def raise_not_implemented
        raise NoMethodError, "This method hasn't been implemented yet."
      end

      def to_dbobject obj
        if obj.respond_to?(:merge)
          hash_to_dbobject(obj)
        elsif obj.respond_to?(:compact)
          array_to_dblist(obj)
        elsif obj.class == Symbol
          obj.to_s
        else
          # primitive value, no conversion necessary
          #puts "Un-handled class type [#{obj.class}]"
          obj
        end
      end

      def from_dbobject obj
        # for better upstream compatibility make the objects into ruby hash or array
        case obj
        when Java::ComMongodb::BasicDBObject
          h = obj.hashify
          Hash[h.keys.zip(h.values.map{|v| from_dbobject(v)})]
        when Java::ComMongodb::BasicDBList
          obj.arrayify.map{|v| from_dbobject(v)}
        when Java::JavaUtil::ArrayList 
          obj.map{|v| from_dbobject(v)}
        when Java::JavaUtil::Date
          Time.at(obj.get_time/1000.0)
        else
          obj
        end
      end

      def sort_value(key, value)
        val = value.to_s.downcase
        return val if val == '2d'
        direction = SortingHash[val]
        return direction if direction != 0
        raise InvalidSortValueError.new(
          "for key: #{key}, #{value} was supplied as a sort direction when acceptable values are: " +
          "Mongo::ASCENDING, 'ascending', 'asc', :ascending, :asc, 1, Mongo::DESCENDING, " +
          "'descending', 'desc', :descending, :desc, -1.")
      end

      SortingHash = Hash.new(0).merge!(
        "ascending" => 1, "asc" => 1, "1" => 1,
        "descending" => -1, "desc" => -1, "-1" => -1
      )

      private

      def hash_to_dbobject doc
        obj = JMongo::BasicDBObject.new
        doc.each_pair do |key, value|
          obj.put(key.to_s, to_dbobject(value))
        end
        obj
      end

      def array_to_dblist ary
        list = [] #Java::ComMongodb::DBObject[ary.length].new
        ary.each_with_index do |ele, i|
          list[i] = to_dbobject(ele)
        end
        list
      end

      #@collection.save({:doc => 'foo'}, :safe => nil)       ---> NONE = new WriteConcern(-1)
      #@collection.save({:doc => 'foo'}, :safe => true)        ---> NORMAL = new WriteConcern(0)
      #@collection.save({:doc => 'foo'}, :safe => {:w => 2})   ---> new WriteConcern( 2 , 0 , false)
      #@collection.save({:doc => 'foo'}, :safe => {:w => 2, :wtimeout => 200})                 ---> new WriteConcern( 2 , 200 , false)
      #@collection.save({:doc => 'foo'}, :safe => {:w => 2, :wtimeout => 200, :fsync => true}) ---> new WriteConcern( 2 , 0 , true)
      #@collection.save({:doc => 'foo'}, :safe => {:fsync => true}) ---> FSYNC_SAFE = new WriteConcern( 1 , 0 , true)

      def write_concern(safe)
        return JMongo::WriteConcern.new(-1) if safe.nil?
        return JMongo::WriteConcern.new(0) if safe.is_a?(FalseClass)
        return JMongo::WriteConcern.new(1) if safe.is_a?(TrueClass)
        return JMongo::WriteConcern.new(0) unless safe.is_a?(Hash)
        w = safe[:w] || 1
        t = safe[:wtimeout] || 0
        f = !!(safe[:fsync] || false)
        JMongo::WriteConcern.new(w, t, f) #dont laugh!
      end
    end
  end
end

