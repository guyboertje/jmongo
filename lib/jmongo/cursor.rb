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

    NEXT_DOCUMENT_TIMEOUT = 0.125

    attr_reader :j_cursor, :collection, :selector, :fields,
      :order, :hint, :snapshot, :timeout,
      :full_collection_name, :transformer,
      :options

    def initialize(collection, options={})
      @collection = collection
      @j_collection = collection.j_collection
      @selector   = convert_selector_for_query(options[:selector])
      @fields     = convert_fields_for_query(options[:fields])
      @admin      = options.fetch(:admin, false)
      @order      = nil
      @batch_size = Mongo::DEFAULT_BATCH_SIZE
      @skip       = 0
      @limit      = 0
      _skip options[:skip]
      _limit options[:limit]
      _sort options[:order]
      _batch_size options[:batch_size]
      _hint options[:hint]
      @snapshot   = options[:snapshot]
      @explain    = options[:explain]
      @socket     = options[:socket]
      @timeout    = options.fetch(:timeout, true)
      @transformer = options[:transformer]
      @tailable   = options.fetch(:tailable, false)
      @do_tailable_timeout = false

      if @tailable
        @await_data  = options.fetch(:await_data, true)
        @next_timeout = NEXT_DOCUMENT_TIMEOUT
        @is_poison_function = nil
        @poison_doc = default_poison_doc
        @do_tailable_timeout = true
        case @await_data
        when Hash
          @poison_doc = @await_data.fetch(:poison_doc, default_poison_doc)
          @is_poison_function = @await_data[:is_poison_function]
          @next_timeout = @await_data.fetch(:next_timeout, NEXT_DOCUMENT_TIMEOUT).to_f
        when Numeric
          @next_timeout = @await_data.to_f
        end
        @timeout_thread = TimeoutThread.new(@collection, @poison_doc, @next_timeout)
      end

      @full_collection_name = "#{@collection.db.name}.#{@collection.name}"

      spawn_cursor
    end

    def rewind!
      close
      spawn_cursor
    end

    def close
      if @j_cursor.num_seen == 0 && !@tailable
        @j_cursor.next rescue nil
      end
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
      raise_invalid_op if @j_cursor.num_seen != 0
      @j_cursor.addOption(opt)
      options
    end

    def options
      @j_cursor.options
    end

    def query_opts
      warn "The method Cursor#query_opts has been deprecated " +
        "and will removed in v2.0. Use Cursor#options instead."
      options
    end

    def remove_option(opt)
      raise_invalid_op if @j_cursor.num_seen != 0
      @j_cursor.setOptions(options & ~opt)
      options
    end

    def current_document
      _xform(from_dbobject(@j_cursor.curr))
    end

    def next_document
      doc = nil
      trap_raise(Mongo::OperationFailure) do
        if @tailable
          doc = __next
        elsif has_next?
          doc = __next
        end
      end
      _xform(doc)
    end
    alias :next :next_document

    def _xform(doc)
      if @transformer && @transformer.respond_to?('call')
        @transformer.call(doc)
      else
        doc
      end
    end
    private :_xform

    def has_next?
      if @tailable
        true
      else
        @j_cursor.has_next?
      end
    end

    # iterate directly from the mongo db
    def each
      while has_next?
        yield next_document
      end
    end

    def _hint(hint = nil)
      return if hint.nil?
      @hint = to_dbobject(hint)
    end

    def _batch_size(size=nil)
      return if size.nil?
      raise ArgumentError, "batch_size requires an integer" unless size.is_a? Integer
      @batch_size = size
    end
    private :_batch_size

    def batch_size(size=nil)
      _batch_size(size)
      @j_cursor = @j_cursor.batchSize(@batch_size) if @batch_size
      self
    end

    def _limit(number_to_return=nil)
      return if number_to_return.nil? && @limit
      raise ArgumentError, "limit requires an integer" unless number_to_return.is_a? Integer
      @limit = number_to_return
    end
    private :_limit

    def limit(number_to_return=nil)
      _limit(number_to_return)
      wrap_invalid_op do
        @j_cursor = @j_cursor.limit(@limit)
      end
      self
    end

    def _skip(number_to_skip=nil)
      return if number_to_skip.nil? && @skip
      raise ArgumentError, "skip requires an integer" unless number_to_skip.is_a? Integer
      @skip = number_to_skip
    end
    private :_skip

    def skip(number_to_skip=nil)
      _skip(number_to_skip)
      wrap_invalid_op do
        @j_cursor = @j_cursor.skip(@skip)
      end
      self
    end

    def _sort(key_or_list=nil, direction=nil)
      return if key_or_list.nil? && @order
      @order = prep_sort(key_or_list, direction)
    end
    private :_sort

    def sort(key_or_list, direction=nil)
      _sort(key_or_list, direction)
      wrap_invalid_op do
        @j_cursor = @j_cursor.sort(@order)
      end
      self
    end

    def size
      @j_cursor.size
    end

    def count(skip_and_limit = false)
      wrap_invalid_op do
        if skip_and_limit && @skip && @limit
          @j_cursor.size
        else
          @j_cursor.count
        end
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

    def done_size
      @j_cursor.num_seen
    end

    def to_do_size
      @j_cursor.size - @j_cursor.num_seen
    end

    private

    def default_poison_doc
      { 'jmongo_poison_document' => true }
    end

    def default_is_poison?(doc)
      !!doc['jmongo_poison_document']
    end

    def __next
      if @do_tailable_timeout
        @timeout_thread.trigger
        doc = from_dbobject(@j_cursor.next)
        if poisoned?(doc)
          nil
        else
          @timeout_thread.cancel
          doc
        end
      else
        from_dbobject(@j_cursor.next)
      end
    end

    def poisoned?(doc)
      if @is_poison_function
        @is_poison_function.call(doc)
      else
        default_is_poison?(doc)
      end
    end

    # Convert the +:fields+ parameter from a single field name or an array
    # of fields names to a hash, with the field names for keys and '1' for each
    # value.
    def convert_fields_for_query(fields)
      to_dbobject prep_fields(fields)
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
        @j_cursor = @j_cursor.hint(@hint) if @hint
        @j_cursor = @j_cursor.snapshot if @snapshot
        @j_cursor = @j_cursor.batchSize(@batch_size) if @batch_size && @batch_size > 0
        @j_cursor = @j_cursor.addOption JMongo::Bytes::QUERYOPTION_NOTIMEOUT unless @timeout
        @j_cursor = @j_cursor.addOption JMongo::Bytes::QUERYOPTION_TAILABLE if @tailable
      end

      self
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
