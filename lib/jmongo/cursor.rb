module Mongo

  class Cursor
    include Mongo::Utils

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

      cursor_opts = to_java_cursor_options(options)

      #@full_collection_name = "#{@collection.db.name}.#{@collection.name}"
      @cache = []
      @closed = false
      @query_run = false

      @j_cursor = spawn_cursor cursor_opts
    end

    def next_document
      from_dbobject(@j_cursor.next)
    end

    def each
      num_returned = 0
      while @j_cursor.has_next? && (@limit <= 0 || num_returned < @limit)
        yield next_document
        num_returned += 1
      end
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

    def to_java_cursor_options options
      @timeout    = options[:timeout]  || false
      @tailable   = options[:tailable] || false

      j_opts = 0
      j_opts = j_opts | JMongo::Bytes::QUERYOPTION_TAILABLE if @tailable
      j_opts = j_opts | JMongo::Bytes::QUERYOPTION_NOTIMEOUT if @timeout
      j_opts
    end

    def spawn_cursor opts
      cursor = if opts.zero?
        @fields.nil? ? @j_collection.find(@selector) :  @j_collection.find(@selector, @fields)
      else
        @j_collection.find @selector,
        @fields,
        @skip,
        @batch_size,
        opts
      end

      if cursor
        cursor = cursor.skip @skip unless @skip.zero?
        cursor = cursor.limit @limite unless @limit.zero?
        #cursor = cursor.batchSize @batch_size
        cursor
      else
        nil
      end
    end

  end # class Cursor

end # module Mongo
