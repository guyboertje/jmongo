# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = %q{jmongo}
  s.version = "0.1.1"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Chuck Remes","Guy Boertje"]
  s.date = %q{2010-05-05}
  s.default_executable = %q{jmongo}
  s.description = %q{Thin jruby wrapper around Mongo Java Driver}
  s.email = %q{cremes@mac.com}
  s.executables = ["jmongo"]
  s.extra_rdoc_files = ["History.txt", "README.txt", "bin/jmongo", "version.txt", "LICENSE.txt"]
  s.files = ["History.txt", "README.txt", "Rakefile", "bin/jmongo", "lib/jmongo.rb", "lib/jmongo_jext.rb", "lib/jmongo/ajrb.rb", "lib/jmongo/collection.rb", "lib/jmongo/connection.rb", "lib/jmongo/cursor.rb", "lib/jmongo/db.rb", "lib/jmongo/utils.rb", "spec/jmongo_spec.rb", "spec/spec_helper.rb", "version.txt"]
  s.files += ["lib/jmongo/mongo-2.2.jar"]
  s.has_rdoc = true
  s.homepage = %q{http://github.com/chuckremes/jmongo}
  s.rdoc_options = ["--main", "README.txt"]
  s.require_paths = ["lib"]
  s.rubyforge_project = %q{jmongo}
  s.rubygems_version = %q{1.3.1}
  s.summary = "Thin ruby wrapper around Mongo Java Driver; for JRuby only"

  if s.respond_to? :specification_version then
    current_version = Gem::Specification::CURRENT_SPECIFICATION_VERSION
    s.specification_version = 2

    if Gem::Version.new(Gem::RubyGemsVersion) >= Gem::Version.new('1.2.0') then
      s.add_development_dependency(%q<bones>, [">= 3.4.1"])
    else
      s.add_dependency(%q<bones>, [">= 3.4.1"])
      s.add_dependency "jrjackson"
    end
  else
    s.add_dependency(%q<bones>, [">= 3.4.1"])
    s.add_dependency "jrjackson"
  end
end
