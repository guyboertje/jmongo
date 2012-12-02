# Copyright (C) 2010 Guy Boertje

module Mongo
  module JavaImpl
    module Db_
      SYSTEM_NAMESPACE_COLLECTION = "system.namespaces"
      SYSTEM_PROFILE_COLLECTION = "system.profile"

      #@collection.save({:doc => 'foo'}, :safe => nil)       ---> NONE = new WriteConcern(-1)
      #@collection.save({:doc => 'foo'}, :safe => true)        ---> NORMAL = new WriteConcern(0)
      #@collection.save({:doc => 'foo'}, :safe => {:w => 2})   ---> new WriteConcern( 2 , 0 , false)
      #@collection.save({:doc => 'foo'}, :safe => {:w => 2, :wtimeout => 200})                 ---> new WriteConcern( 2 , 200 , false)
      #@collection.save({:doc => 'foo'}, :safe => {:w => 2, :wtimeout => 200, :fsync => true}) ---> new WriteConcern( 2 , 0 , true)
      #@collection.save({:doc => 'foo'}, :safe => {:fsync => true}) ---> FSYNC_SAFE = new WriteConcern( 1 , 0 , true)

      def write_concern(safe_)
        self.class.write_concern(safe_ || self.safe || @connection.write_concern)
      end

      private

      def exec_command(cmd)
        cmd_hash = cmd.kind_of?(Hash) ? cmd : {cmd => 1}
        cmd_res = @j_db.command(to_dbobject(cmd_hash))
        from_dbobject cmd_res
      end

      def do_eval(string, *args)
        command(BSON::OrderedHash['$eval', string,'args', args])
      end

      def collection_exists?(name)
        system_name?(name) || @j_db.collection_exists(name)
      end

      def get_last_error
        from_dbobject @j_db.get_last_error
      end

      def _collections_info(coll_name=nil)
        selector = {}
        selector[:name] = full_collection_name(coll_name) if coll_name
        coll = @j_db.get_collection(SYSTEM_NAMESPACE_COLLECTION)
        from_dbobject(coll.find(to_dbobject(selector)))
      end
    end
  end
end
