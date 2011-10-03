module Mongo
  ASCENDING  =  1
  DESCENDING = -1
  GEO2D      = '2d'

  DEFAULT_MAX_BSON_SIZE = 1024 * 1024 * 4

  REPLACE  = JMongo::MapReduceCommand::OutputType::REPLACE
  MERGE    = JMongo::MapReduceCommand::OutputType::MERGE
  REDUCE   = JMongo::MapReduceCommand::OutputType::REDUCE
  INLINE   = JMongo::MapReduceCommand::OutputType::INLINE

  MapReduceEnumHash = {:replace => REPLACE, :merge => MERGE, :reduce => REDUCE, :inline => INLINE}

  DEFAULT_BATCH_SIZE = 100

  OP_REPLY        = 1
  OP_MSG          = 1000
  OP_UPDATE       = 2001
  OP_INSERT       = 2002
  OP_QUERY        = 2004
  OP_GET_MORE     = 2005
  OP_DELETE       = 2006
  OP_KILL_CURSORS = 2007

  OP_QUERY_TAILABLE          = JMongo::Bytes::QUERYOPTION_TAILABLE
  OP_QUERY_SLAVE_OK          = JMongo::Bytes::QUERYOPTION_SLAVEOK
  OP_QUERY_OPLOG_REPLAY      = JMongo::Bytes::QUERYOPTION_OPLOGREPLAY
  OP_QUERY_NO_CURSOR_TIMEOUT = JMongo::Bytes::QUERYOPTION_NOTIMEOUT
  OP_QUERY_AWAIT_DATA        = JMongo::Bytes::QUERYOPTION_AWAITDATA
  OP_QUERY_EXHAUST           = JMongo::Bytes::QUERYOPTION_EXHAUST

  REPLY_CURSOR_NOT_FOUND     = JMongo::Bytes::RESULTFLAG_CURSORNOTFOUND
  REPLY_QUERY_FAILURE        = JMongo::Bytes::RESULTFLAG_ERRSET
  REPLY_SHARD_CONFIG_STALE   = JMongo::Bytes::RESULTFLAG_SHARDCONFIGSTALE
  REPLY_AWAIT_CAPABLE        = JMongo::Bytes::RESULTFLAG_AWAITCAPABLE

  def self.logger(logger=nil)
    logger ? @logger = logger : @logger
  end

  def self.result_ok?(result)
    result['ok'] == 1.0 || result['ok'] == true
  end

  # Simple class for comparing server versions.
  class ServerVersion
    include Comparable

    def initialize(version)
      @version = version
    end

    # Implements comparable.
    def <=>(new)
      local, new  = self.to_a, to_array(new)
      for n in 0...local.size do
        break if elements_include_mods?(local[n], new[n])
        if local[n] < new[n].to_i
          result = -1
          break;
        elsif local[n] > new[n].to_i
          result = 1
          break;
        end
      end
      result || 0
    end

    # Return an array representation of this server version.
    def to_a
      to_array(@version)
    end

    # Return a string representation of this server version.
    def to_s
      @version
    end

    private

    # Returns true if any elements include mod symbols (-, +)
    def elements_include_mods?(*elements)
      elements.any? { |n| n =~ /[\-\+]/ }
    end

    # Converts argument to an array of integers,
    # appending any mods as the final element.
    def to_array(version)
      array = version.split(".").map {|n| (n =~ /^\d+$/) ? n.to_i : n }
      if array.last =~ /(\d+)([\-\+])/
        array[array.length-1] = $1.to_i
        array << $2
      end
      array
    end
  end
end
