#!/usr/bin/env ruby
# %RUBY create weekly tables

require "date"
require "erb"
require "yaml"

def dates(year, cweek)
  start = Date.new(year, 1, 4)
  start -= (start.cwday - 1)
  start += (7 * (cweek - 1))
  start..(start + 6)
end

year = 2016
cweek = 2

rows = YAML.load(File.read(ARGV[0]))
cols = dates(year, cweek)

File.open(sprintf("%d-W%02d.html", year, cweek), "w") do |ous|
  ous << ERB.new(DATA.read, nil, "-").result(binding)
end

__END__
<html>
 <head>
  <title>-</title>
 </head>
 <body>
  <table border="1">
   <tr>
    <th colspan="2">-</th>
    <%- cols.each do |date| -%>
    <th><%= date.strftime("%Y-%m-%d") %></th>
    <%- end -%>
   </tr>
   <%- rows.each do |row| -%>
   <%- if row.has_key?("detail") -%>
   <%- header = true -%>
   <%- row["detail"].each do |detail| -%>
   <tr>
    <%- if header -%>
    <th rowspan="<%= row["detail"].size %>"><%= row["name"] %></th>
    <%- header = false -%>
    <%- end -%>
    <th><%= detail["name"] %></th>
    <%- cols.each do |date| -%>
    <td><img src="<%= row["prefix"] %>_<%= detail["prefix"] %>_<%= date.strftime("%Y-%m-%d") %>.png" /></td>
    <%- end -%>
   </tr>
   <%- end -%>
   <%- else -%>
   <tr>
    <th colspan="2"><%= row["name"] %></th>
    <%- cols.each do |date| -%>
    <td><img src="<%= row["prefix"] %>_<%= date.strftime("%Y-%m-%d") %>.png" /></td>
    <%- end -%>
   </tr>
   <%- end -%>
   <%- end -%>
  </table>
 </body>
</html>
