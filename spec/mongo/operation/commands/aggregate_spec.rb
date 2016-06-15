require 'spec_helper'

describe Mongo::Operation::Commands::Aggregate do
  include_context 'operation'

  let(:selector) do
    { :aggregate => coll_name,
      :pipeline => []
    }
  end
  let(:spec) do
    { :selector => selector,
      :options => {},
      :db_name => db_name
    }
  end
  let(:op) { described_class.new(spec) }


  describe '#initialize' do

    context 'spec' do

      it 'sets the spec' do
        expect(op.spec).to be(spec)
      end
    end
  end

  describe '#==' do

    context ' when two ops have different specs' do
      let(:other_selector) do
        { :aggregate => 'another_test_coll',
          :pipeline => [],
        }
      end
      let(:other_spec) do
        { :selector => other_selector,
          :options => options,
          :db_name => db_name,
        }
      end
      let(:other) { described_class.new(other_spec) }

      it 'returns false' do
        expect(op).not_to eq(other)
      end
    end
  end

  describe '#execute' do

    context 'when the aggregation fails' do

      let(:selector) do
        { :aggregate => coll_name,
          :pipeline => [{ '$invalid' => 'operator' }],
        }
      end

      it 'raises an exception' do
        expect {
          op.execute(authorized_primary.context)
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end
  end

  describe '#update_selector' do

    let(:pipeline) do
      [ {} ]
    end

    let(:context) do
      authorized_client.cluster.next_primary.context
    end

    context 'when the server supports commands with write concern', if: command_write_concern_enabled? do

      let(:selector) do
        { :aggregate => coll_name,
          :pipeline => pipeline,
          :writeConcern => { w: WRITE_CONCERN[:w] + 1 }
        }
      end

      context 'when the pipeline includes the $out operator' do

        let(:pipeline) do
          [{
             "$group" => {
               "_id" => "$city",
               "totalpop" => { "$sum" => "$pop" }
             }
           },
           {
             :$out => 'output_collection'
           }
          ]
        end

        let(:expected_write_concern) do
          { w: WRITE_CONCERN[:w] + 1 }
        end

        it 'includes writeConcern in the selector' do
          expect(op.send(:update_selector, context)[:writeConcern]).to eq(expected_write_concern)
        end
      end

      context 'when the pipeline does not include the $out operator' do

        it 'does not include writeConcern in the selector' do
          expect(op.send(:update_selector, context)[:writeConcern]).to be(nil)
        end
      end
    end

    context 'when the server does not support commands taking write concerns', unless: command_write_concern_enabled? do

      let(:selector) do
        { :aggregate => coll_name,
          :pipeline => pipeline,
          :writeConcern => { w: WRITE_CONCERN[:w] + 1 }
        }
      end

      it 'does not include writeConcern in the selector' do
        expect(op.send(:update_selector, context)[:writeConcern]).to be(nil)
      end
    end

    context 'when the server is a mongos' do

      let(:selector) do
        { :aggregate => coll_name,
          :pipeline => pipeline
        }
      end

      before do
        allow(context).to receive(:mongos?).and_return(true)
      end

      context 'when the read preference requires a special selector' do

        let(:read_pref) do
          Mongo::ServerSelector.get({ mode: :secondary })
        end

        let(:spec) do
          { :selector => selector,
            :options => options,
            :db_name => db_name,
            :read => read_pref
          }
        end

        it 'uses a special selector' do
          expect(op.send(:update_selector, context)[:$readPreference]).to eq(read_pref.to_mongos)
          expect(op.send(:update_selector, context)[:$query]).to eq(selector)
        end
      end

      context 'when the read preference does not require a special selector' do

        let(:read_pref) do
          Mongo::ServerSelector.get({ mode: :primary })
        end

        let(:spec) do
          { :selector => selector,
            :options => options,
            :db_name => db_name,
            :read => read_pref
          }
        end

        it 'does not use a special selector' do
          expect(op.send(:update_selector, context)[:$readPreference]).to be(nil)
          expect(op.send(:update_selector, context)[:$query]).to be(nil)
          expect(op.send(:update_selector, context)).to eq(selector)
        end
      end
    end
  end
end
