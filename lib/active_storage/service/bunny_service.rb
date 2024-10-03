# frozen_string_literal: true

require 'active_support/core_ext/numeric/bytes'
require 'bunnyrb'

module ActiveStorage
  # Wraps the BunnyCDN Storage as an Active Storage service.
  # See ActiveStorage::Service for the generic API documentation that applies to all services.
  class Service::BunnyService < Service

    attr_reader :client

    def initialize(edge_name:, edge_region:, edge_api_token:, cdn_zone:, **options)
      @edge_name = edge_name
      @cdn_zone = cdn_zone

      Bunny.configure do |config|
        config.edge_name = edge_name
        config.edge_region = edge_region
        config.edge_api_token = edge_api_token
      end

      super(**options)
    end

    def upload(key, io, checksum: nil, filename: nil, content_type: nil, disposition: nil, **)
      instrument :upload, key: key, checksum: checksum do
        content_disposition = content_disposition_with(filename: filename, type: disposition) if disposition && filename
        upload_with_single_part key, io, checksum: checksum, content_type: content_type, content_disposition: content_disposition
      end
    end

    def download(key, &block)
      instrument :download, key: key do
        object_for(key).get_file.to_s.force_encoding(Encoding::BINARY)
      rescue StandardError
        raise ActiveStorage::FileNotFoundError
      end
    end

    def delete(key)
      instrument :delete, key: key do
        Bunny::Edge::Upload.delete(name: key)
      end
    end

    def delete_prefixed(prefix)
      instrument :delete_prefixed, prefix: prefix do
        begin
          Bunny::Edge::Upload.delete(path: prefix)
        rescue
          # do nothing
        end
      end
    end

    def exist?(key)
      instrument :exist, key: key do |payload|
        answer = object_for(key).exists?
        payload[:exist] = answer
        answer
      end
    end

    private

    def private_url(key, expires_in:, filename:, disposition:, content_type:, **)
      # BunnyStorageClient does not natively support this operation yet.
      public_url(key)
    end

    def public_url(key)
      File.join("https://#{@cdn_zone}.b-cdn.net", key)
    end

    def upload_with_single_part(key, io, checksum: nil, content_type: nil, content_disposition: nil, custom_metadata: {})
      Bunny::Edge::Upload.create(name: key, file: io)
    rescue StandardError
      raise ActiveStorage::IntegrityError
    end

    def stream_file(key)
      # BunnyStorageClient does not natively support this operation yet.
    end

    # def object_for(key)
    #   client.object(key)
    # end
  end
end
