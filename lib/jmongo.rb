# Copyright (C) 2010 Chuck Remes
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

unless RUBY_PLATFORM =~ /java/
  error "This gem is only compatible with a java-based ruby environment like JRuby."
  exit 255
end

require 'require_all'
require_rel 'jmongo/*.jar'

# import all of the java packages we'll need into the JMongo namespace
require 'jmongo/jmongo_jext'
require_rel 'jmongo/*.rb'

module Mongo
  ASCENDING  =  1
  DESCENDING = -1
  GEO2D      = '2d'

  DEFAULT_MAX_BSON_SIZE = 1024 * 1024 * 4

  REPLACE  = JMongo::MapReduceCommand::OutputType::REPLACE
  MERGE    = JMongo::MapReduceCommand::OutputType::MERGE
  REDUCE   = JMongo::MapReduceCommand::OutputType::REDUCE
  INLINE   = JMongo::MapReduceCommand::OutputType::INLINE

  module Constants
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
  end
end
