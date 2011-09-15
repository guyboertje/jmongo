Gem::Specification.new do |s|
  s.name              = 'jmongo'
  s.version           = '1.0.0'
  s.date              = '2011-09-15'
  s.platform          = Gem::Platform::RUBY
  s.authors           = ["Chuck Remes","Guy Boertje", "Lee Henson"]
  s.email             = ["cremes@mac.com", "guyboertje@gmail.com", "lee.m.henson@gmail.com"]
  s.summary           = "Thin ruby wrapper around Mongo Java Driver; for JRuby only"
  s.description       = %q{Thin jruby wrapper around Mongo Java Driver}
  s.homepage          = 'http://github.com/guyboertje/jmongo'

  s.executables       =  ["jmongo"]
  s.extra_rdoc_files  = ["History.txt", "README.txt", "bin/jmongo", "LICENSE.txt"]
  s.rdoc_options      = ["--main", "README.txt"]
  s.rubyforge_project = %q{jmongo}
  s.test_files        = `git ls-files -- {test,spec,features}/*`.split("\n")

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
    lib/jmongo/ajrb.rb
    lib/jmongo/collection.rb
    lib/jmongo/connection.rb
    lib/jmongo/cursor.rb
    lib/jmongo/db.rb
    lib/jmongo/exceptions.rb
    lib/jmongo/jmongo_jext.rb
    lib/jmongo/mongo-2.6.3.jar
    lib/jmongo/utils.rb
    lib/jmongo/version.rb
    spec/jmongo_spec.rb
    spec/spec_helper.rb
  ]
  # = MANIFEST =

  s.add_development_dependency 'awesome_print', '~> 0.4'
  s.add_development_dependency 'fuubar',        '~> 0.0'
  s.add_development_dependency 'rspec',         '~> 2.6'
end
