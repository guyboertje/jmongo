# Copyright (C) 2010 Guy Boertje

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
  end
end
