require 'date'
require 'rspec/core/rake_task'

#############################################################################
#
# Helper functions
#
#############################################################################

def name
  @name ||= Dir['*.gemspec'].first.split('.').first
end

def version
  line = File.read("lib/#{name}/version.rb")[/^\s*VERSION\s*=\s*.*/]
  line.match(/.*VERSION\s*=\s*['"](.*)['"]/)[1]
end

def date
  Date.today.to_s
end

def rubyforge_project
  name
end

def gemspec_file
  "#{name}.gemspec"
end

def gem_file
  "#{name}-#{version}.gem"
end

def replace_header(head, header_name)
  head.sub!(/(\.#{header_name}\s*= ').*'/) { "#{$1}#{send(header_name)}'"}
end

def jruby?
  RUBY_PLATFORM.to_s == 'java'
end

#############################################################################
#
# Custom tasks
#
#############################################################################

default_rspec_opts = %w[--colour --format Fuubar]

desc "Run all examples"
RSpec::Core::RakeTask.new(:spec) do |t|
  t.rspec_opts = default_rspec_opts
end

#############################################################################
#
# Packaging tasks
#
#############################################################################

desc "Create tag v#{version} and build and push #{gem_file} to Rubygems"
task :release => :build do
  unless `git branch` =~ /^\* master$/
    puts "You must be on the master branch to release!"
    exit!
  end
  sh "git commit --allow-empty -a -m 'Release #{version}'"
  sh "git tag v#{version}"
  sh "git push origin master"
  sh "git push origin v#{version}"

  command = "gem push pkg/#{name}-#{version}.gem"

  if jruby?
    puts "--------------------------------------------------------------------------------------"
    puts "can't push to rubygems using jruby at the moment, so switch to mri and run: #{command}"
    puts "--------------------------------------------------------------------------------------"
  else
    sh command
  end
end

desc "Build #{gem_file} into the pkg directory"
task :build => :gemspec do
  sh "mkdir -p pkg"
  sh "gem build #{gemspec_file}"
  sh "mv #{gem_file} pkg"
end

desc "Generate #{gemspec_file}"
task :gemspec => :validate do
  # read spec file and split out manifest section
  spec = File.read(gemspec_file)
  head, manifest, tail = spec.split("  # = MANIFEST =\n")

  # replace name version and date
  replace_header(head, :name)
  replace_header(head, :version)
  replace_header(head, :date)
  #comment this out if your rubyforge_project has a different name
  #replace_header(head, :rubyforge_project)

  # determine file list from git ls-files
  files = `git ls-files`.
    split("\n").
    sort.
    reject { |file| file =~ /^\./ }.
    reject { |file| file =~ /^(rdoc|pkg)/ }.
    map { |file| "    #{file}" }.
    join("\n")

  # piece file back together and write
  manifest = "  s.files = %w[\n#{files}\n  ]\n"
  spec = [head, manifest, tail].join("  # = MANIFEST =\n")
  File.open(gemspec_file, 'w') { |io| io.write(spec) }
  puts "Updated #{gemspec_file}"
end

desc "Validate #{gemspec_file}"
task :validate do
  libfiles = Dir['lib/*'] - ["lib/#{name}.rb", "lib/#{name}"]
  unless libfiles.empty?
    puts "Directory `lib` should only contain a `#{name}.rb` file and `#{name}` dir."
    exit!
  end
  unless Dir['VERSION*'].empty?
    puts "A `VERSION` file at root level violates Gem best practices."
    exit!
  end
end

require 'rake/testtask'

task :test do
  puts "\nTo test the pure jruby driver: \nrake test:jruby\n\n"
end

namespace :test do

  desc "Test the driver using pure jruby (no C extension)"
  task :jruby do
    ENV['C_EXT'] = nil
    if ENV['TEST']
      Rake::Task['test:functional'].invoke
    else
      Rake::Task['test:unit'].invoke
      Rake::Task['test:functional'].invoke
      Rake::Task['test:bson'].invoke
      Rake::Task['test:pooled_threading'].invoke
      Rake::Task['test:drop_databases'].invoke
    end
  end

  desc "Run the replica set test suite"
  Rake::TestTask.new(:rs) do |t|
    t.test_files = FileList['test/replica_sets/*_test.rb']
    t.verbose    = true
    t.ruby_opts << '-w'
  end

  Rake::TestTask.new(:unit) do |t|
    t.test_files = FileList['test/unit/*_test.rb']
    t.verbose    = true
    t.ruby_opts << '-w'
  end

  Rake::TestTask.new(:functional) do |t|
    t.test_files = FileList['test/*_test.rb']
    t.verbose    = true
    t.ruby_opts << '-w'
  end

  Rake::TestTask.new(:pooled_threading) do |t|
    t.test_files = FileList['test/threading/*_test.rb']
    t.verbose    = true
    t.ruby_opts << '-w'
  end

  Rake::TestTask.new(:auto_reconnect) do |t|
    t.test_files = FileList['test/auxillary/autoreconnect_test.rb']
    t.verbose    = true
    t.ruby_opts << '-w'
  end

  Rake::TestTask.new(:authentication) do |t|
    t.test_files = FileList['test/auxillary/authentication_test.rb']
    t.verbose    = true
    t.ruby_opts << '-w'
  end

  Rake::TestTask.new(:new_features) do |t|
    t.test_files = FileList['test/auxillary/1.4_features.rb']
    t.verbose    = true
    t.ruby_opts << '-w'
  end

  Rake::TestTask.new(:bson) do |t|
    t.test_files = FileList['test/bson/*_test.rb']
    t.verbose    = true
    t.ruby_opts << '-w'
  end
end
