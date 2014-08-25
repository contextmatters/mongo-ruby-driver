# Copyright (C) 2009-2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'test_helper'
require 'pp'
include Mongo

class ShardedClusterTest < Test::Unit::TestCase
  TEST_DB = 'sharded_cluster_test'
  TEST_COLL = 'sharded_cluster_test'

  @@mo = Mongo::Orchestration::Service.new

  def setup
    @cluster = @@mo.configure({:orchestration => 'sh', :request_content => {:id => 'sharded_cluster_1', :preset => 'basic.json'} })
    @cluster.start
    @seed = 'mongodb://' + @cluster.object['uri']
    @client = Mongo::MongoClient.from_uri(@seed)
    #@client.drop_database(TEST_DB)
    @db = @client[TEST_DB]
    @coll = @db[TEST_COLL]
  end

  def teardown
    @coll.remove({})
    #@client.drop_database(TEST_DB)
  end

  test 'Sharded cluster mongos failover' do
    @coll.insert({'a' => 1})
    assert_equal([1], @coll.find({}, :sort => [['a', Mongo::ASCENDING]]).to_a.map{|doc| doc['a']})
    # pending
    @coll.insert({'a' => 3})
    assert_equal([1, 3], @coll.find({}, :sort => [['a', Mongo::ASCENDING]]).to_a.map{|doc| doc['a']})
  end
end

