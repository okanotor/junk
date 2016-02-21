#!/usr/bin/env ruby

require "yaml"
require_relative "./sardata_importer"

config = SardataImporter::Stocker::DEFINITIONS
col_name_ip_addr = SardataImporter::Writer::PostgreSQL::COLUMN_NAME_IP_ADDRESS
col_name_timestamp = SardataImporter::Writer::PostgreSQL::COLUMN_NAME_TIMESTAMP

config.each do |stocker_config|
  buffer = []
  case ARGV[0]
  when "DROP" then buffer << "DROP TABLE IF EXISTS sar_#{stocker_config[:type]} RESTRICT;"
  when "TRUNCATE" then buffer << "TRUNCATE sar_#{stocker_config[:type]};"
  else
    buffer << "CREATE TABLE sar_#{stocker_config[:type]} ("
    buffer << "  id BIGSERIAL NOT NULL PRIMARY KEY"
    buffer << "  , #{col_name_ip_addr} TEXT NOT NULL"
    if stocker_config[:dev_name]
      buffer << "  , #{stocker_config[:dev_name]} TEXT NOT NULL"
    end
    buffer << "  , #{col_name_timestamp} TIMESTAMP WITH TIME ZONE NOT NULL"
    stocker_config[:fields].values.each do |column|
      type = case column[:data_type]
             when :numeric 
               "NUMERIC(#{column[:precision]}, #{column[:scale]})"
             when :long
               "BIGINT"
             else
               "TEXT"
             end
      buffer << "  , #{column[:col_name]} #{type}"
    end
    buffer << ");"
  end
  puts buffer.join("\n")
end
