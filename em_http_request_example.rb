require 'rubygems'

require 'nokogiri'
require 'pry'

require 'eventmachine'
require 'em-http'
require 'pp'

# Ruby HTTP clients comparison (speakerdeck.com)
# http://lanyrd.com/2012/rubyconf/szpth/
# Full table from slides
# https://docs.google.com/spreadsheets/d/1uS3UbQR6GaYsozaF5yQMLmkySY6TO42BIndr2hUW2L4/pub?hl=en&hl=en&output=html

urls = [ "http://novosibirsk.drom.ru/renault/logan/20436777.html",
        "http://novosibirsk.drom.ru/volvo/xc90/17834089.html",
        "http://novosibirsk.drom.ru/audi/a4/20434269.html" ]

pending = urls.size

EM.run do
  urls.each do |url|
    http = EM::HttpRequest.new(url).get
    http.callback {
      puts "#{url}\n#{http.response_header.status} - #{http.response.length} bytes\n"
      doc = Nokogiri::HTML(http.response)
      puts doc.css('h1').text.strip

      pending -= 1
      EM.stop if pending < 1
    }
    http.errback {
      puts "#{url}\n" + http.error

      pending -= 1
      EM.stop if pending < 1
    }
  end
end

# ------------------------------------------------------------

# puts "Synchronizing with Multi interface"
# EventMachine.run {
#   multi = EventMachine::MultiRequest.new

#   reqs = [ "http://novosibirsk.drom.ru/renault/logan/20436777.html",
#           "http://novosibirsk.drom.ru/volvo/xc90/17834089.html",
#           "http://novosibirsk.drom.ru/audi/a4/20434269.html" ]

#   reqs.each_with_index do |url, idx|
#     http = EventMachine::HttpRequest.new(url, :connect_timeout => 1)
#     req = http.get
#     multi.add idx, req
#   end

#   multi.callback  do
#     p multi.responses[:callback].size
#     p multi.responses[:errback].size

#     multi.responses[:callback].each do |ind,cb|
#       doc = Nokogiri::HTML(cb.response)
#       puts doc.css('h1').text.strip
#     end

#     EventMachine.stop
#   end
# }

# ------------------------------------------------------------

# EventMachine.run {
#     http = EventMachine::HttpRequest.new(url).get # :query => {'keyname' => 'value'}

#     http.errback { p 'Uh oh'; EM.stop }
#     http.callback {
#       p http.response_header.status
#       p http.response_header
#       p http.response
#       EventMachine.stop
#     }
#   }
# }
