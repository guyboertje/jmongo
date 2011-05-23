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
      private
      def get_db_names
        @connection.get_database_names
      end
      def drop_a_db(name)
        @connection.drop_database(name)
      end
      def _server_version
        @connection.get_version
      end
    end

    module Db_
      SYSTEM_NAMESPACE_COLLECTION = "system.namespaces"
      #SYSTEM_INDEX_COLLECTION = "system.indexes"
      #SYSTEM_PROFILE_COLLECTION = "system.profile"
      #SYSTEM_USER_COLLECTION = "system.users"
      #SYSTEM_COMMAND_COLLECTION = "$cmd"

      private
      def exec_command(cmd)
        cmd_res = @j_db.command(to_dbobject({cmd => true}))
        from_dbobject(cmd_res)
      end
      def has_coll(name)
        @j_db.collection_exists(name)
      end
    end

    module Collection_
      private
      def create_indexes(obj,opts)
        return @j_collection.ensureIndex("#{obj}") if obj.is_a?(String) || obj.is_a?(Symbol)

        obj = Hash[obj] if obj.is_a?(Array)

        return @j_collection.ensureIndex(to_dbobject(obj),to_dbobject(opts)) if opts.is_a?(Hash)
        @j_collection.ensureIndex(to_dbobject(obj),generate_index_name(obj),!!(opts))
      end

      def remove_documents(obj,safe)
        if safe
          wr = @j_collection.remove(to_dbobject(obj),write_concern(:safe))
        else
          wr = @j_collection.remove(to_dbobject(obj))
        end
        wr.get_error.nil? && wr.get_n > 0
      end

      def insert_documents(obj,safe)
        db_obj = to_dbobject(obj)
        if safe && !db_obj.kind_of?(java.util.ArrayList)
          @j_collection.insert(db_obj,write_concern(:safe))
        else
          @j_collection.insert(db_obj)
        end
        obj.collect { |o| o['_id'] || o[:_id] }
      end

      def find_and_modify_document(query,fields,sort,remove,update,new_,upsert)
        #res =
        @j_collection.find_and_modify(to_dbobject(query),to_dbobject(fields),to_dbobject(sort),remove,to_dbobject(update),new_,upsert)
        #puts res.inspect
        #from_dbobject
        #res
      end

      def find_one_document(document,fields)
        @j_collection.findOne(to_dbobject(document),to_dbobject(fields))
        #from_dbobject
      end

      def update_documents(selector,document,upsert=false,multi=false)
        @j_collection.update(to_dbobject(selector),to_dbobject(document),upsert,multi)
      end

      def save_document(obj, safe)
        id = obj.delete(:_id) || obj.delete('_id')
        obj['_id'] = id || BSON::ObjectId.new
        db_obj = to_dbobject(obj)
        if safe
          @j_collection.save(db_obj,write_concern(:safe))
        else
          @j_collection.save(db_obj)
        end
        obj['_id']
      end
    end

    module Utils
      def raise_not_implemented
        raise NoMethodError, "This method hasn't been implemented yet."
      end
      
      def to_dbobject obj
        case obj
        when Array
          array_to_dblist obj
        when Hash
          hash_to_dbobject obj
        else
          # primitive value, no conversion necessary
          #puts "Un-handled class type [#{obj.class}]"
          obj
        end
      end

      def from_dbobject obj
        # it appears that JRuby automagically wraps the java objects and makes them
        # accessible to us for "free"; no need to do any special conversion
        # but the jruby wrapper object has been reopened to have more 'hash like' methods
        obj
      end

      private

      def hash_to_dbobject doc
        obj = JMongo::BasicDBObject.new
        doc.each_pair do |key, value|
          obj.put(key.to_s, to_dbobject(value))
        end
        obj
      end

      def array_to_dblist ary
        list = java.util.ArrayList.new
        ary.each do |ele|
          list.add to_dbobject(ele)
        end
        list
      end

      WRT_CONCERN = Hash.new(0).merge!({:safe=>1})
      def write_concern(kind=nil)
        JMongo::WriteConcern.new(WRT_CONCERN[kind])
      end
    end
  end
end

