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

      def write_concern(safe)
        return @j_db.write_concern if safe.nil?
        return JMongo::WriteConcern.new(0) if safe.is_a?(FalseClass)
        return JMongo::WriteConcern.new(1) if safe.is_a?(TrueClass)
        return JMongo::WriteConcern.new(0) unless safe.is_a?(Hash)
        w = safe[:w] || 1
        t = safe[:wtimeout] || 0
        f = !!(safe[:fsync] || false)
        JMongo::WriteConcern.new(w, t, f) #dont laugh!
      end

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

      def get_last_error
        from_dbobject @j_db.get_last_error
      end
    end
  end
end
