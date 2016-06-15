require 'spec_helper'

describe Mongo::Collection::View::Builder::Aggregation do

  describe '#specification' do

    let(:view) do
      Mongo::Collection::View.new(authorized_collection, {}, options)
    end

    let(:pipeline) do
      [{
         "$group" => {
             "_id" => "$city",
             "totalpop" => { "$sum" => "$pop" }
         }
       }
      ]
    end

    let(:builder) do
      described_class.new(pipeline, view, options)
    end

    let(:specification) do
      builder.specification
    end

    let(:selector) do
      specification[:selector]
    end

    context 'when write concern is passed as an option' do

      let(:options) do
        BSON::Document.new({ write_concern: { w: WRITE_CONCERN[:w] + 1 } })
      end

      context 'when the view has a write concern' do

        let(:expected_write_concern) do
          BSON::Document.new(Mongo::WriteConcern.get(options[:write_concern]).options)
        end

        it 'uses the write concern option' do
          expect(specification[:selector][:writeConcern]).to eq(expected_write_concern)
        end
      end

      context 'when the view does not have a write concern' do

        let(:view) do
          authorized_client.with(write: nil)[authorized_collection.name].find({}, options)
        end

        let(:expected_write_concern) do
          BSON::Document.new(Mongo::WriteConcern.get(options[:write_concern]).options)
        end

        it 'uses the write concern option' do
          expect(specification[:selector][:writeConcern]).to eq(expected_write_concern)
        end
      end
    end

    context 'when write concern is not passed as an option' do

      let(:options) { {} }

      context 'when the view has a write concern' do

        let(:expected_write_concern) do
          BSON::Document.new(view.write_concern.options)
        end

        it 'uses the write concern on the view' do
          expect(specification[:selector][:writeConcern]).to eq(expected_write_concern)
        end
      end

      context 'when the view does not have a write concern' do

        let(:view) do
          authorized_client.with(write: nil)[authorized_collection.name].find({}, options)
        end

        it 'does not set any write concern' do
          expect(specification[:selector][:writeConcern]).to be(nil)
        end
      end
    end
  end
end
