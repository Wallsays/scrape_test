require 'rubygems'
require 'typhoeus'
require 'nokogiri'
require 'pry'
require 'pp'

urls = [ #{}"http://novosibirsk.drom.ru/renault/logan/20436777.html",
        "http://auto.drom.ru/region54/toyota/?go_search=2&minyear=2003&maxyear=2005&minprice=600000&maxprice=800000&transmission=0&privod=0&order=year",
        # "http://novosibirsk.drom.ru/volvo/xc90/17834089.html",
        # "http://novosibirsk.drom.ru/audi/a4/20434269.html",
        # "http://novosibirsk.drom.ru/nissan/cefiro/20388447.html",
        # "http://novosibirsk.drom.ru/nissan/terrano/17120410.html" 
      ]

$options = {
  followlocation: true,
  # proxy: 'http://myproxy.org',
  # proxy: 'http://proxyurl.com', proxyuserpwd: 'user:password'
}

def parse_callback(url, response)
  puts "#{url}\n#{response.code} - #{response.body.length} bytes\n"
  puts "queued_requests.size: #{$hydra.queued_requests.size}"
  doc = Nokogiri::HTML(response.body)
  # puts doc.css('h1').text.strip
  pager = doc.css('.pager')
  # binding.pry
  if pager && (pager.css('> a').text == "Следующая" || pager.css('> a:last').text == "Следующая" )
    puts "Next page present"
    # binding.pry
    page = pager.css(' > a:last').attribute('href').value
    url = page
    nested_req = Typhoeus::Request.new(url, $options)
    nested_req.on_complete do |response|
      parse_callback(url, response)
    end
    $hydra.queue nested_req
    puts "queued_requests.size: #{$hydra.queued_requests.size}"
  end
end

$hydra = Typhoeus::Hydra.new(max_concurrency: 20)
urls.each do |url|
  request = Typhoeus::Request.new(url, $options)
  request.on_complete do |response|
    parse_callback(url, response)
  end
  $hydra.queue(request)
end
$hydra.run