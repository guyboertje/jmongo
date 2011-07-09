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

  class Cursor
    include Mongo::JavaImpl::Utils

    attr_reader :j_cursor

    def initialize(collection, options={})
      @j_collection = collection.j_collection

      @selector   = convert_selector_for_query(options[:selector])
      @fields     = convert_fields_for_query(options[:fields])
      @admin      = options[:admin]    || false
      @skip       = options[:skip]     || 0
      @limit      = options[:limit]    || 0
      @order      = options[:order]
      @hint       = options[:hint]
      @snapshot   = options[:snapshot]
      @explain    = options[:explain]
      @socket     = options[:socket]
      @batch_size = options[:batch_size] || Mongo::Constants::DEFAULT_BATCH_SIZE
      @timeout    = options[:timeout]  || false
      @tailable   = options[:tailable] || false

      #@full_collection_name = "#{@collection.db.name}.#{@collection.name}"
      @query_run = false
      spawn_cursor
    end
    def current_document
      if !@query_run
        next_document
      else
        from_dbobject(@j_cursor.curr)
      end
    end

    def next_document
      @query_run = true
      @j_cursor.has_next? ? from_dbobject(@j_cursor.next) : BSON::OrderedHash.new
    end

    def has_next?
      @j_cursor.has_next?
    end
    # iterate directly from the mongo db
    def each
      check_modifiable
      while @j_cursor.has_next?
        yield next_document
      end
    end

    def limit(number_to_return=nil)
      return @limit unless number_to_return
      check_modifiable
      raise ArgumentError, "limit requires an integer" unless number_to_return.is_a? Integer

      @limit = number_to_return
      @j_cursor = @j_cursor.limit(@limit)
      self
    end

    def skip(number_to_skip=nil)
      return @skip unless number_to_skip
      check_modifiable
      raise ArgumentError, "skip requires an integer" unless number_to_skip.is_a? Integer

      @skip = number_to_skip
      @j_cursor = @j_cursor.skip(@skip)
      self
    end

    def sort(key_or_list, direction=nil)
      check_modifiable

      if !direction.nil?
        order = [[key_or_list, direction]]
      else
        order = [key_or_list]
      end
      ord = Hash[*order.flatten]
      @j_cursor = @j_cursor.sort(to_dbobject(ord))
      self
    end

    def size
      @j_cursor.size
    end

    def count(skip_and_limit = false)
      if skip_and_limit && @skip && @limit
        check_modifiable
        @j_cursor.skip(@skip).limit(@limit).size
      else
        @j_cursor.size
      end
    end

    def explain
      from_dbobject @j_cursor.explain
    end

    def map(&block)
      ret = []
      check_modifiable
      while @j_cursor.has_next?
        ret << block.call(from_dbobject(@j_cursor.next))
      end
      ret
    end

    def to_a
      ret = []
      check_modifiable
      while @j_cursor.has_next?
        ret << from_dbobject(@j_cursor.next)
      end
      ret
    end
    private

    # Convert the +:fields+ parameter from a single field name or an array
    # of fields names to a hash, with the field names for keys and '1' for each
    # value.
    def convert_fields_for_query(fields)
      case fields
      when String, Symbol
        to_dbobject({fields => 1})
      when Array
        return nil if fields.length.zero?
        hash = {}
        fields.each { |field| hash[field] = 1 }
        to_dbobject hash
      when Hash
        to_dbobject fields
      end
    end

    # Set the query selector hash.
    def convert_selector_for_query(selector)
      case selector
      when Hash
        to_dbobject selector
      when nil
        to_dbobject({})
      end
    end

    def spawn_cursor
      @j_cursor = @fields.nil? || @fields.empty? ? @j_collection.find(@selector) :  @j_collection.find(@selector, @fields)

      if @j_cursor
        @j_cursor = @j_cursor.sort(@order) if @order
        @j_cursor = @j_cursor.skip(@skip) if @skip > 0
        @j_cursor = @j_cursor.limit(@limit) if @limit > 0
        @j_cursor = @j_cursor.batchSize(@batch_size)

        @j_cursor = @j_cursor.addOption JMongo::Bytes::QUERYOPTION_NOTIMEOUT unless @timeout
        @j_cursor = @j_cursor.addOption JMongo::Bytes::QUERYOPTION_TAILABLE if @tailable
      end

      self
    end

    def check_modifiable
      if @query_run
        raise "Cannot modify the query once it has been run or closed."
      end
    end

  end # class Cursor

end # module Mongo
