%w{req router dealer rep}.each do |what|
  require File.expand_path("../test_#{what}.rb", __FILE__)
end
