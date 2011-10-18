Gem::Specification.new do |s|
  s.name              = 'jmongo'
  s.version           = '1.1.2'
  s.date              = '2011-10-18'
  s.platform          = Gem::Platform::RUBY
  s.authors           = ["Chuck Remes","Guy Boertje", "Lee Henson"]
  s.email             = ["cremes@mac.com", "guyboertje@gmail.com", "lee.m.henson@gmail.com"]
  s.summary           = "Thin ruby wrapper around Mongo Java Driver; for JRuby only"
  s.description       = %q{Thin jruby wrapper around Mongo Java Driver}
  s.homepage          = 'http://github.com/guyboertje/jmongo'

  # = MANIFEST =
  s.files = %w[
    Gemfile
    Gemfile.lock
    History.txt
    LICENSE.txt
    README.txt
    Rakefile
    bin/jmongo
    jmongo.gemspec
    lib/jmongo.rb
    lib/jmongo/collection.rb
    lib/jmongo/connection.rb
    lib/jmongo/cursor.rb
    lib/jmongo/db.rb
    lib/jmongo/exceptions.rb
    lib/jmongo/mongo-2.6.5.gb1.jar
    lib/jmongo/mongo/bson.rb
    lib/jmongo/mongo/collection.rb
    lib/jmongo/mongo/connection.rb
    lib/jmongo/mongo/db.rb
    lib/jmongo/mongo/jmongo.rb
    lib/jmongo/mongo/mongo.rb
    lib/jmongo/mongo/ruby_ext.rb
    lib/jmongo/mongo/utils.rb
    lib/jmongo/version.rb
    spec/jmongo_spec.rb
    spec/spec_helper.rb
    test/auxillary/1.4_features.rb
    test/auxillary/authentication_test.rb
    test/auxillary/autoreconnect_test.rb
    test/auxillary/fork_test.rb
    test/auxillary/repl_set_auth_test.rb
    test/auxillary/slave_connection_test.rb
    test/auxillary/threaded_authentication_test.rb
    test/bson/binary_test.rb
    test/bson/bson_test.rb
    test/bson/byte_buffer_test.rb
    test/bson/hash_with_indifferent_access_test.rb
    test/bson/json_test.rb
    test/bson/object_id_test.rb
    test/bson/ordered_hash_test.rb
    test/bson/test_helper.rb
    test/bson/timestamp_test.rb
    test/collection_test.rb
    test/connection_test.rb
    test/conversions_test.rb
    test/cursor_fail_test.rb
    test/cursor_message_test.rb
    test/cursor_test.rb
    test/data/empty_data
    test/data/sample_data
    test/data/sample_file.pdf
    test/data/small_data.txt
    test/db_api_test.rb
    test/db_connection_test.rb
    test/db_test.rb
    test/grid_file_system_test.rb
    test/grid_io_test.rb
    test/grid_test.rb
    test/load/thin/config.ru
    test/load/thin/config.yml.template
    test/load/thin/load.rb
    test/load/unicorn/config.ru
    test/load/unicorn/load.rb
    test/load/unicorn/unicorn.rb.template
    test/replica_sets/connect_test.rb
    test/replica_sets/connection_string_test.rb
    test/replica_sets/count_test.rb
    test/replica_sets/insert_test.rb
    test/replica_sets/pooled_insert_test.rb
    test/replica_sets/query_secondaries.rb
    test/replica_sets/query_test.rb
    test/replica_sets/read_preference_test.rb
    test/replica_sets/refresh_test.rb
    test/replica_sets/replication_ack_test.rb
    test/replica_sets/rs_test_helper.rb
    test/safe_test.rb
    test/support/hash_with_indifferent_access.rb
    test/support/keys.rb
    test/support_test.rb
    test/test_helper.rb
    test/threading/threading_with_large_pool_test.rb
    test/threading_test.rb
    test/tools/auth_repl_set_manager.rb
    test/tools/keyfile.txt
    test/tools/repl_set_manager.rb
    test/unit/collection_test.rb
    test/unit/connection_test.rb
    test/unit/cursor_test.rb
    test/unit/db_test.rb
    test/unit/grid_test.rb
    test/unit/node_test.rb
    test/unit/pool_manager_test.rb
    test/unit/pool_test.rb
    test/unit/read_test.rb
    test/unit/safe_test.rb
    test/uri_test.rb
  ]
  # = MANIFEST =

  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }

  s.add_dependency 'require_all',                 '~> 1.2'
  s.add_development_dependency 'awesome_print',   '~> 0.4'
  s.add_development_dependency 'fuubar',          '~> 0.0'
  s.add_development_dependency 'rspec',           '~> 2.6'
end
