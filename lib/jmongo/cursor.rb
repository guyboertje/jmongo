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

    attr_reader :j_cursor, :collection, :selector, :fields,
      :order, :hint, :snapshot, :timeout,
      :full_collection_name, :transformer,
      :options

    def initialize(collection, options={})
      @collection = collection
      @j_collection = collection.j_collection
      @query_run = false
      @selector   = convert_selector_for_query(options[:selector])
      @fields     = convert_fields_for_query(options[:fields])
      @admin      = options.fetch(:admin, false)
      @order      = nil
      @batch_size = Mongo::Constants::DEFAULT_BATCH_SIZE
      @skip       = 0
      @limit      = 0
      _skip options[:skip]
      _limit options[:limit]
      _sort options[:order]
      _batch_size options[:batch_size]
      @hint       = options[:hint]
      @snapshot   = options[:snapshot]
      @explain    = options[:explain]
      @socket     = options[:socket]
      @timeout    = options.fetch(:timeout, true)
      @tailable   = options.fetch(:tailable, false)
      @transformer = options[:transformer]

      @full_collection_name = "#{@collection.db.name}.#{@collection.name}"

      spawn_cursor
    end

    def rewind!
      close
      @query_run = false
      spawn_cursor
    end

    def close
      @query_run = true
      @j_cursor.close
    end

    def cursor_id
      @j_cursor.get_cursor_id
    end

    def closed?
      cursor_id == 0
    end

    def alive?
      cursor_id != 0
    end

    def add_option(opt)
      check_modifiable
      @j_cursor.addOption(opt)
      options
    end

    def options
      @j_cursor.getOptions
    end

    def query_opts
      warn "The method Cursor#query_opts has been deprecated " +
        "and will removed in v2.0. Use Cursor#options instead."
      options
    end

    def remove_option(opt)
      check_modifiable
      @j_cursor.setOptions(options & ~opt)
      options
    end

    def current_document
      _xform(from_dbobject(@j_cursor.curr))
    end

    def next_document
      _xform(has_next? ? __next : BSON::OrderedHash.new)
    end
    alias :next :next_document

    def _xform(doc)
      if @transformer.nil?
        doc
      else
        @transformer.call(doc) if doc
      end
    end
    private :_xform

    def has_next?
      @j_cursor.has_next?
    end

    # iterate directly from the mongo db
    def each
      check_modifiable
      while has_next?
        yield next_document
      end
    end

    def _batch_size(size=nil)
      return if size.nil?
      check_modifiable
      raise ArgumentError, "Invalid value for batch_size #{size}; must be 0 or > 1." if size < 0 || size == 1
      @batch_size = @limit != 0 && size > @limit ? @limit : size
    end
    private :_batch_size

    def batch_size(size=nil)
      _batch_size(size)
      @j_cursor = @j_cursor.batchSize(@batch_size) if @batch_size
      self
    end

    def _limit(number_to_return=nil)
      return if number_to_return.nil?
      check_modifiable
      raise ArgumentError, "limit requires an integer" unless number_to_return.is_a? Integer
      @limit = number_to_return
    end
    private :_limit

    def limit(number_to_return=nil)
      _limit(number_to_return)
      wrap_invalid_op do
        @j_cursor = @j_cursor.limit(@limit) if @limit
      end
      self
    end

    def _skip(number_to_skip=nil)
      return if number_to_skip.nil?
      check_modifiable
      raise ArgumentError, "skip requires an integer" unless number_to_skip.is_a? Integer
      @skip = number_to_skip
    end
    private :_skip

    def skip(number_to_skip=nil)
      _skip(number_to_skip)
      wrap_invalid_op do
        @j_cursor = @j_cursor.skip(@skip) if @skip
      end
      self
    end

    def sort(key_or_list, direction=nil)
      _sort(key_or_list, direction)
      wrap_invalid_op do
        @j_cursor = @j_cursor.sort(@order) if @order
      end
      self
    end

    def _sort(key_or_list=nil, direction=nil)
      return if key_or_list.nil?
      check_modifiable
      if !direction.nil?
        order = [[key_or_list, direction]]
      elsif key_or_list.is_a?(String) || key_or_list.is_a?(Symbol)
        order = [key_or_list.to_s, 1]
      else
        order = [key_or_list]
      end
      @order = to_dbobject(Hash[*order.flatten])
    end
    private :_sort

    def size
      @j_cursor.size
    end

    def count(skip_and_limit = false)
      if skip_and_limit && @skip && @limit
        check_modifiable
        @j_cursor.size
      else
        @j_cursor.count
      end
    end

    def explain
      from_dbobject @j_cursor.explain
    end

    def map(&block)
      ret = []
      rewind! unless has_next?
      while has_next?
        ret << block.call(__next)
      end
      ret
    end

    def to_a
      ret = []
      rewind! unless has_next?
      while has_next?
        ret << __next
      end
      ret
    end

    def to_set
      Set.new self.to_a
    end

    private

    def __next
      @query_run = true
      from_dbobject(@j_cursor.next)
    end

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

    def no_fields?
      @fields.nil? || @fields.empty?
    end

    def spawn_cursor
      @j_cursor = no_fields? ? @j_collection.find(@selector) :  @j_collection.find(@selector, @fields)

      if @j_cursor
        @j_cursor = @j_cursor.sort(@order) if @order
        @j_cursor = @j_cursor.skip(@skip) if @skip && @skip > 0
        @j_cursor = @j_cursor.limit(@limit) if @limit && @limit > 0
        @j_cursor = @j_cursor.batchSize(@batch_size) if @batch_size && @batch_size > 0

        @j_cursor = @j_cursor.addOption JMongo::Bytes::QUERYOPTION_NOTIMEOUT unless @timeout
        @j_cursor = @j_cursor.addOption JMongo::Bytes::QUERYOPTION_TAILABLE if @tailable
      end

      self
    end

    def check_modifiable
      if @query_run
        raise_invalid_op
      end
    end

    def wrap_invalid_op
      begin
        yield
      rescue => ex
        raise_invalid_op
      end
    end

    def raise_invalid_op
      raise InvalidOperation, "Cannot modify the query once it has been run or closed."
    end
  end # class Cursor

end # module Mongo
