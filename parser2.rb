require 'rubygems'
require 'typhoeus'
require 'nokogiri'
require "sequel"
require 'pry'
require 'pp'

DB = Sequel.connect('postgres://localhost/car_monitor_development')
$dataset = DB[:cars]
$brands_set = DB[:brands]
$models_set = DB[:models]

domain = "auto.drom.ru"
# city = '' 
# region = '' 
region = 'region54'
firms = {
  # 'toyota' => [], 
  'mazda' => [], 
  'nissan' => [],
  'honda' => [], 
  'mitsubishi' => [], 
  'volkswagen' => [], 
  'lexus' => [],
  'subaru' => [],
  'suzuki' => [],
  'infiniti' => [],
  # 'hyundai' => ["coupe", "tiburon", "tuscani"],
  'hyundai' => [],
  'kia' => [],
  'chevrolet' => [],
  'ford' => [],
  'mercedes-benz'=> [],
  'bmw' => [],
  'audi' => [],
  'volvo' => [],
  'renault' => [],
  'skoda' => [],
  'opel' => [],
  'peugeot' => [],
  'land_rover' => [],
  'ssang_yong' => [],
  'citroen' => []
}
min_year = 1995
# min_year = 2015
# max_year = 1995
max_year = 2015
minprice = 200000
maxprice = 2000000
# transmission_id = 2 # autoamatic
# privod = 2 # 1 - передний, 2 - задний, 3 - 4WD 
model_id ||= nil
transmission_id ||= 0 
minprice ||= 0
maxprice ||= 0
privod ||= 0
min_year ||= 0 
max_year ||= 0 
page = ''

BRAND_LIST = [
  "Toyota", "Nissan", "Honda", "Mitsubishi", "Hyundai",
  "--------------",
  "Acura", "Alfa Romeo", "Alpina", "Aston Martin", "Audi", 
  "BMW", "Brilliance", "Buick",  
  "Cadillac", "Changan", "Chery", "Chevrolet", "Chrysler", "Citroen", 
  "Dacia", "Daewoo", "Daihatsu", "Daimler", "Datsun", "Dodge", "Dongfeng", 
  "FAW", "Fiat", "Ford", 
  "Geely", "GMC", "Great Wall", 
  "Hafei", "Haima", "Honda", "Hummer", "Hyundai", "Haval",
  "Infiniti", "Isuzu", "Jaguar", "Jeep", "Kia", 
  "Land Rover", "Lexus", "Lifan", "Lincoln", "Luxgen",
  "Mazda", "Mercedes-Benz", "Mitsubishi", #, "Mini"
  "Nissan", "Opel", "Peugeot", "Plymouth", "Pontiac", "Porsche", "Proton",
  "Renault", "Rover", 
  "Saab", "Saturn", "Scion", "SEAT", "Skoda", "Smart", "SsangYong", "Subaru", "Suzuki", 
  "Tianye", "Toyota", "Volkswagen", "Volvo"
]

cars = []
current_car = Hash.new
firm_cnt = 0
model_cnt = 0

def generate_url(parse_options)
  # all cities
  # http://auto.drom.ru/hyundai/coupe/?minyear=2007&transmission=2&order=year&go_search=2
  # Nsk reg
  # http://auto.drom.ru/region54/hyundai/coupe/?minyear=2007&transmission=2&order=year&go_search=2007
  # Nsk 
  # http://novosibirsk.drom.ru/hyundai/coupe/?minyear=2007&transmission=2&order=year&go_search=2007

  # "http://#{domain}/#{region}#{firm_id}/#{model_id[model_cnt]}#{page}/?go_search=2&minyear=#{min_year}&transmission=#{transmission_id}&privod=#{privod}&order=year"
  url = 'http://'
  url += if parse_options[:region]
    'auto.drom.ru/region54/'
  elsif parse_options[:city]
    'novosibirsk.drom.ru/'
  else
    'auto.drom.ru/'
  end

  url += if parse_options[:firm_id]
    parse_options[:firm_id] + '/'
  end

  url += if parse_options[:model_id] && !parse_options[:model_id].empty?
    parse_options[:model_id][ parse_options[:model_cnt] ] + '/'
  else
    ''
  end

  url += "?go_search=2"

  url += if parse_options[:min_year]
    "&minyear=#{parse_options[:min_year]}"
  end

  url += if parse_options[:max_year]
    "&maxyear=#{parse_options[:max_year]}"
  end

  url += if parse_options[:minprice]
    "&minprice=#{parse_options[:minprice]}"
  end

  url += if parse_options[:maxprice]
    "&maxprice=#{parse_options[:maxprice]}"
  end

  url += if parse_options[:transmission_id]
    "&transmission=#{parse_options[:transmission_id]}"
  end

  url += if parse_options[:privod]
    "&privod=#{parse_options[:privod]}"
  end

  url += "&order=year"
end

def scrape_table_row(item, firms, firm_id, model_id)
  date = Date.strptime( item.css('td:nth-child(1) a').text , '%d-%m')
  link = item.css('td:nth-child(2) img').first.parent.attribute('href').value
  preview = item.css('td:nth-child(2) img').attribute('src').value
  model = item.css('td:nth-child(3)').text.strip.squeeze(' ')
  equipment_name = item.css('td:nth-child(3) small').text.strip.squeeze(' ')
  model.sub!(equipment_name, '').strip if equipment_name.length > 0
  sold = item.css('td:nth-child(3) strike').text.size > 0 ? true : false
  model.sub!('сертифицирован', '')

  new_car = item.css('td > .b-sticker.b-sticker_theme_new').empty? ? false : true

  brand = model.split(" ").first
  if model_id && !model_id.empty?
    BRAND_LIST.any? do |word| 
      if model.include?(word)
        brand = word
        model = model.sub(brand,'').strip
      end
    end
  else
    # if scrape by brand w/o models
    brand = firm_id.capitalize.sub('_',' ')
  end
       
  year = item.css('td:nth-child(4)').text.delete(' ').to_i
  hp = item.css('td:nth-child(5) .gray').text
  info = item.css('td:nth-child(5)').text.strip.squeeze(' ').sub(hp,'')
  volume = info.include?('л') ? info[0..4].to_f : 0.0
  transmission = info.include?("автомат") ? "автомат" : info.include?("механика") ? "механика" : ""
  wir_dr = info.include?("4WD") ? "4WD" : info.include?("передний") ? "передний" : info.include?("задний") ? "задний" : ""

  kms = item.css('td:nth-child(6)').text.delete(' ').to_i

  no_docs = false
  broken = false
  extra = item.css('td:nth-child(7) img')
  unless extra.empty?
    extra.each do |img|
      no_docs = true if img.attribute('title').value == "Без документов"
      broken  = true if img.attribute('title').value == "Битый или не на ходу"
    end
  end

  brd = $brands_set.filter(title: brand)
  if brd.first.nil?
    $brands_set.insert({
      title: brand,
      created_at: DateTime.now,
      updated_at: DateTime.now,
      slug: brand.downcase.gsub(' ', '_').gsub("'", '')
    })
    brd = $brands_set.filter(title: brand)
  end
  # binding.pry
  model.strip!
  mdl = $models_set.filter(title: model, brand_id: brd.first[:id])
  if mdl.first.nil?
    $models_set.insert({
      title: model, 
      brand_id: brd.first[:id], 
      # rate: model_rate,
      created_at: DateTime.now,
      updated_at: DateTime.now,
      slug: model.downcase.gsub(' ', '_').gsub("'", '')
    })
    mdl = $models_set.filter(title: model, brand_id: brd.first[:id])
  end

  cost = item.css('td:nth-child(8) .f14').text.delete(' ').to_i
  city = item.css('td:nth-child(8) span:last').text.delete(' ')
  current_car = {
    equipment_name: equipment_name,
    brand_id: brd.first[:id],
    model_id: mdl.first[:id],
    new_car: new_car,
    date: date,
    source_url: link,
    preview_url: preview,
    model: model,
    brand: brand,
    year: year,
    horsepower: hp.delete('(').to_i,
    engine_v: volume,
    transmission: transmission,
    wheel_drive: wir_dr,
    odometer: kms*1000,
    cost: cost,
    city: city,
    sold: sold,
    no_docs: no_docs,
    broken: broken,
    created_at: DateTime.now,
    updated_at: DateTime.now,
    row_parsed_at: DateTime.now,
    origin_site_id: 1 # drom
  }
end

def insert_data(item, firms, firm_id, model_id)
  current_car = scrape_table_row(item, firms, firm_id, model_id)
  # cars << current_car
  # binding.pry
  car = $dataset.where(source_url: current_car[:source_url]).first
  unless car
    current_car[:price_initial] = current_car[:cost]
    current_car[:price_current] = current_car[:cost]
    current_car[:new_price_date] = DateTime.now
    $dataset.insert(current_car)
    $cnt += 1
  else
    if car[:sold] != current_car[:sold]
      car = $dataset.filter(id: car[:id])
      car.update(
        sold: current_car[:sold],
        row_parsed_at: current_car[:row_parsed_at]
      )
      $upd_cnt += 1
    elsif car[:price_current] != current_car[:cost]
      car = $dataset.filter(id: car[:id])
      car.update(
        price_current:  current_car[:cost],
        price_previous: car.first[:price_current], 
        new_price_date: DateTime.now,
        row_parsed_at: current_car[:row_parsed_at]
      )
      $price_cnt += 1
    end
  end
end




def parse_callback(url, response, parse_options)
  # binding.pry
  $cnt = 0
  $upd_cnt = 0
  $price_cnt = 0
  print url
  # puts 
  # binding.pry
  # puts "queued_requests.size: #{$hydra.queued_requests.size}"
  doc = Nokogiri::HTML(response.body)
  unless doc.css('.subscriptions_link_wrapper').empty?
    doc.css('.subscriptions_link_wrapper').first.parent.css('tr.row').each do |item|
        insert_data(item, parse_options[:firms], parse_options[:firm_id], parse_options[:model_id])
    end
    doc.css('.subscriptions_link_wrapper').first.parent.css('tr.h').each do |item|
        insert_data(item, parse_options[:firms], parse_options[:firm_id], parse_options[:model_id])
    end
  end
  print " : #{$cnt} : #{$upd_cnt} : #{$price_cnt}\n"
  pager = doc.css('.pager')
  # binding.pry
  if pager && (pager.css('> a').text == "Следующая" || pager.css('> a:last').text == "Следующая" )
    url = pager.css(' > a:last').attribute('href').value
    queue_req(url, parse_options)
  elsif parse_options[:model_id] && parse_options[:model_cnt] < ((parse_options[:model_id].size) - 1)
    parse_options[:model_cnt] += 1
    url = generate_url(parse_options)
    queue_req(url, parse_options)
  end
end

def queue_req(url, parse_options)
  req = Typhoeus::Request.new(url, $options)
  req.on_complete do |response|
    parse_callback(url, response, parse_options)
  end
  $hydra.queue req
end

$options = {
  followlocation: true,
  # proxy: 'http://myproxy.org',
  # proxy: 'http://proxyurl.com', proxyuserpwd: 'user:password'
}
$hydra = Typhoeus::Hydra.new(max_concurrency: 20)

run_table_search = true
if run_table_search
  beg_year = min_year
  end_year = max_year
  year_step = 2
  beg_price = minprice
  end_price = maxprice
  price_step = 200000 # 200 k
  firms.each do |firm, models|
    firm_id = firm
    model_cnt = 0
    model_id = models #[model_cnt]

    (beg_price..end_price).step( price_step ) do |price|
      minprice = price
      maxprice = price + price_step
      maxprice = end_price if maxprice > end_price
      next if maxprice == minprice && end_price != beg_price
      (beg_year..end_year).step( year_step ) do |year|
        min_year = year
        max_year = year + year_step
        max_year = Date.today.year if max_year > Date.today.year
        next if max_year == min_year && end_year != beg_year
        parse_options = {}
        %w(region firms firm_id firm_cnt model_id model_cnt min_year max_year minprice maxprice transmission_id privod).each do |opt|
          parse_options[opt.to_sym] = eval(opt)
        end
        url = generate_url(parse_options)
        queue_req(url, parse_options)
      end
    end

  end
  $hydra.run
end

