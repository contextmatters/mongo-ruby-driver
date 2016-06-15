# Copyright (C) 2014-2015 MongoDB, Inc.
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

module Mongo
  module Operation
    module Commands

      # A MongoDB aggregate operation.
      #
      # @note An aggregate operation can behave like a read and return a 
      #   result set, or can behave like a write operation and
      #   output results to a user-specified collection.
      #
      # @example Create the aggregate operation.
      #   Aggregate.new({
      #     :selector => {
      #       :aggregate => 'test_coll', :pipeline => [{ '$out' => 'test-out' }]
      #     },
      #     :db_name => 'test_db'
      #   })
      #
      # Initialization:
      #   param [ Hash ] spec The specifications for the operation.
      #
      #   option spec :selector [ Hash ] The aggregate selector.
      #   option spec :db_name [ String ] The name of the database on which
      #     the operation should be executed.
      #   option spec :options [ Hash ] Options for the aggregate command.
      #
      # @since 2.0.0
      class Aggregate < Command

        private

        def filter_cursor_option(sel, context)
          return sel if context.features.write_command_enabled?
          sel.reject{ |option, value| option.to_s == 'cursor' }
        end

        def filter_write_concern_option(sel, context)
          return sel unless sel[:writeConcern] || sel['writeConcern']
          return sel if context.features.command_write_concern_enabled? &&
              sel[:pipeline].any? { |op| op.key?('$out') || op.key?(:$out) }
          sel.reject{ |option, value| option.to_s == 'writeConcern' }
        end

        def filter_for_mongos(sel, context)
          if context.mongos? && read_pref = read.to_mongos
            s = sel[:$query] ? sel : { :$query => sel }
            s.merge(:$readPreference => read_pref)
          else
            sel
          end
        end

        def update_selector(context)
          filtered_selector = filter_cursor_option(selector, context)
          filtered_selector = filter_write_concern_option(filtered_selector, context)
          filtered_selector = filter_for_mongos(filtered_selector, context)
          filtered_selector
        end
      end
    end
  end
end

require 'mongo/operation/commands/aggregate/result'
