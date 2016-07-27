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

require 'set'

module Mongo
  class Cluster

    # A manager that sends kill cursors operations at regular intervals to close
    # cursors that have been garbage collected without being exhausted.
    #
    # @since 2.3.0
    class CursorManager

      # The default time interval for the cursor manager to send kill cursors operations.
      #
      # @since 2.3.0
      FREQUENCY = 1.freeze

      # Create a cursor manager.
      #
      # @example Create a CursorManager.
      #   Mongo::Cluster::CursorManager.new
      #
      # @api private
      #
      # @since 2.3.0
      def initialize
        @to_kill = {}
        @active_cursors = Set.new
        @mutex = Mutex.new
      end

      # Start the cursor manager's reaper thread.
      #
      # @example Start the cursor manager's reaper thread.
      #   manager.run
      #
      # @api private
      #
      # @since 2.3.0
      def run
        @reaper ||= Thread.new(FREQUENCY) do |i|
          loop do
            sleep(i)
            kill_cursors
          end
        end
      end

      # Schedule a kill cursors operation to be eventually executed.
      #
      # @example Schedule a kill cursors operation.
      #   manager.schedule_kill_cursor(id, op_spec, server)
      #
      # @param [ Integer ] id The id of the cursor to kill.
      # @param [ Hash ] op_spec The spec for the kill cursors op.
      # @param [ Mongo::Server ] server The server to send the kill cursors operation to.
      #
      # @api private
      #
      # @since 2.3.0
      def schedule_kill_cursor(id, op_spec, server)
        @mutex.synchronize do
          if @active_cursors.include?(id)
            @to_kill[server] ||= Set.new
            @to_kill[server] << op_spec
          end
        end
      end

      # Register a cursor id as active.
      #
      # @example Register a cursor as active.
      #   manager.register_cursor(id)
      #
      # @param [ Integer ] id The id of the cursor to register as active.
      #
      # @api private
      #
      # @since 2.3.0
      def register_cursor(id)
        if id && id > 0
          @mutex.synchronize do
            @active_cursors << id
          end
        end
      end

      # Unregister a cursor id, indicating that it's no longer active.
      #
      # @example Unregister a cursor.
      #   manager.unregister_cursor(id)
      #
      # @param [ Integer ] id The id of the cursor to unregister.
      #
      # @api private
      #
      # @since 2.3.0
      def unregister_cursor(id)
        @mutex.synchronize do
          @active_cursors.delete(id)
        end
      end

      # Execute all pending kill cursors operations.
      #
      # @example Execute pending kill cursors operations.
      #   manager.kill_cursors
      #
      # @api private
      #
      # @since 2.3.0
      def kill_cursors
        cursors_to_kill = {}

        @mutex.synchronize do
          cursors_to_kill = @to_kill.dup
          @to_kill = {}
        end

        cursors_to_kill.each do |server, op_specs|

          op_specs.each do |op_spec|
            if server.features.find_command_enabled?
              Operation::Commands::Command.new(op_spec).execute(server.context)
            else
              Operation::KillCursors.new(op_spec).execute(server.context)
            end
          end
        end
      end
    end
  end
end
