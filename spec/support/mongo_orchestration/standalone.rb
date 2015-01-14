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

module MongoOrchestration
  class Standalone
    include Requestable

    ORCHESTRATION = 'servers'

    attr_reader :client
    attr_reader :id

    # Stop this resource.
    #
    # @since 2.0.0
    def stop
      request_content = { body: { action: 'stop' } }
      @config = post("#{ORCHESTRATION}/#{id}", request_content)
      self
    end

    def run
      setup
      @phases.each do |phase|
        run_phase(phase)
      end
    end

    private

    def run_phase(phase)
      if phase['tests']
        phase['tests'].each do |test|
          begin
            if test['operation'] == "insertOne"
              @collection.insert_one(test['doc'])
            else
              @collection.find.count
            end
          rescue => ex
            raise ex unless test['outcome']['ok'] == 0
          end
          raise Exception unless test['outcome']['ok'] == 1
        end
      elsif phase['MO-operations']
      elsif phase['client-operations']
      end
    end

    def setup
      setup = @spec['setup'] || {}
      @collection ||= @client[@spec['collection'] || TEST_COLL]
      setup['operations'].each do |op|
        if op['action'] == 'insertOne'
          @collection.insert_one(op['doc'])
        else
          @collection.find.count
        end
      end
      @phases = @spec['phases']
    end

    def create(options = {})
      unless alive?
        @config = post(ORCHESTRATION,
                      { body: create_body })
        @id = @config['id']
        @client = Mongo::Client.new("#{@config['mongodb_uri']}")
      end
      self
    end

    def create_body
      body = { name: 'mongod' }
      body.merge!(@spec && @spec['init_config'] ? @spec['init_config'] :
        { procParams: { journal: true } })
    end
  end
end