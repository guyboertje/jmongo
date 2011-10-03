# Copyright (C) 2010 Guy Boertje

module Mongo
  module JavaImpl
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

      def get_last_error
        from_dbobject @j_db.get_last_error
      end

    end
  end
end
