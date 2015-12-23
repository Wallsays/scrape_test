require 'rubygems'
require 'typhoeus'
require "sequel"
require 'nokogiri'
require 'capybara'
require 'capybara/dsl'
require 'capybara/poltergeist'
require 'pry'
require 'pp'

# for Antigate
require 'net/http'
require 'net/http/post/multipart'

class AntigateAPIClient

  DEFAULT_API_URL = 'http://antigate.com'

  def initialize(key)
    @key = key
  end

  def send_captcha( captcha_file )
    uri = URI.parse( "#{DEFAULT_API_URL}/in.php" )
    file = File.new( captcha_file, 'rb' )
    req = Net::HTTP::Post::Multipart.new( uri.path,
                                          :method => 'post',
                                          :key => @key,
                                          :file => UploadIO.new( file, 'image/jpeg', 'image.jpg' ),
                                          :is_russian => 1)
    http = Net::HTTP.new( uri.host, uri.port )
    begin
      resp = http.request( req )
    rescue => err
      puts err
      return nil
    end
    
    id = resp.body
    return id[ 3..id.size ]
  end

  def get_captcha_text( id )
    data = { :key => @key,
             :action => 'get',
             :id => id,
             :min_len => 5,
             :max_len => 5 }
    uri = URI.parse( "#{DEFAULT_API_URL}/res.php" )
    req = Net::HTTP::Post.new( uri.path )
    http = Net::HTTP.new( uri.host, uri.port )
    req.set_form_data( data )

    begin
      resp = http.request(req)
    rescue => err
      puts err
      return nil
    end
    
    text = resp.body
    if text != "CAPCHA_NOT_READY"
      return text[ 3..text.size ]
    end
    return nil
  end

  def report_bad( id )
    data = { :key => @key,
             :action => 'reportbad',
             :id => id }
    uri = URI.parse( "#{DEFAULT_API_URL}/res.php" )
    req = Net::HTTP::Post.new( uri.path )
    http = Net::HTTP.new( uri.host, uri.port )
    req.set_form_data( data )

    begin
      resp = http.request(req)
    rescue => err
      puts err
    end
  end
end #class Antigate

run_table_search = run_details_scrape = run_email_phone_parse = false
ARGV.each do|a|
  run_table_search   = true if a.to_i == 1
  run_details_scrape = true if a.to_i == 2
  run_email_phone_parse = true if a.to_i == 3
end

DB = Sequel.connect('postgres://localhost/car_monitor_development')
$dataset    = DB[:cars]
$brands_set = DB[:brands]
$models_set = DB[:models]

domain = "auto.drom.ru"
# city = '' 
# region = '' 
region = 'region54'
firms = {
  'toyota' => [], 
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

# -------------------------------------------------------
# Scrape table search cars
# -------------------------------------------------------
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

def parse_phone(doc)
  phone = if !doc.css('strong > a.b-link:contains("Посмотреть карточку продавца")').empty?
    unless doc.css('span:contains("Телефон")').empty?
      doc.css('span:contains("Телефон")').first.next_sibling.text.strip
    else
      unless doc.css('span:contains("Контакт")').empty?
        doc.css('span:contains("Контакт")').first.next_sibling.text.strip
      else
        doc.css('.b-media-cont__label.b-media-cont__label_no-wrap').text
      end
    end
  else
    if doc.css('.b-media-cont__label.b-media-cont__label_no-wrap').empty?
      unless doc.css('span:contains("Телефон")').empty?
        doc.css('span:contains("Телефон")').first.next_sibling.text.strip
      end
      unless doc.css('span:contains("Контакт")').empty?
        doc.css('span:contains("Контакт")').first.next_sibling.text.strip
      end
    else
      doc.css('.b-media-cont__label.b-media-cont__label_no-wrap').text
    end
  end
end

def detailed_scrape(car, response)
    puts "#{car[:id]} : #{car[:source_url]}"
    doc = Nokogiri::HTML(response.body)
    item = doc.css('.adv-text .b-media-cont.b-media-cont_relative').first
    # binding.pry
    if item
      petrol = unless item.css('span:contains("Двигатель")').empty?
        item.css('span:contains("Двигатель")').first.next_sibling.text.strip.split(",")[0]
      end
      color = unless item.css('span:contains("Цвет")').empty?
        item.css('span:contains("Цвет")').first.next_sibling.text.strip
      end
      kms = unless item.css('span:contains("Пробег, км")').empty?
        item.css('span:contains("Пробег, км")').first.next_sibling.text.strip.to_i
      end
      new_car = unless item.css('span:contains("Пробег:")').empty?
        item.css('span:contains("Пробег:")').first.next_sibling.next_sibling.text.include?("Новый")
      else
        false
      end
      kms_not_in_rus = unless item.css('span:contains("Пробег по России:")').empty?
        item.css('span:contains("Пробег по России:")').first.next_sibling.text.include?("без пробега")
      else
        false
      end
      steer_wheel = unless item.css('span:contains("Руль")').empty?
        item.css('span:contains("Руль")').first.next_sibling.text.strip
      end
      body_type = unless item.css('span:contains("Тип кузова")').empty?
        item.css('span:contains("Тип кузова")').first.next_sibling.text.strip
      end
      exchange = unless doc.css('span:contains("Обмен")').empty?
        doc.css('span:contains("Обмен")').first.next_sibling.text.strip
      end
      equipment_url = nil
      equipment_name = unless doc.css('span:contains("Комплектация:")').empty?
        equipment_url = doc.css('span:contains("Комплектация:")').first.next_sibling.next_sibling.attribute('href').value
        doc.css('span:contains("Комплектация:")').first.next_sibling.next_sibling.text.strip
      end
      details = unless item.parent.css('span:contains("Дополнительно")').empty?
        item.parent.css('span:contains("Дополнительно")').first.parent.text.sub('Дополнительно:', '').gsub(/\r?\n/, '<br>')
      end
      tuning = unless item.parent.css('span:contains("Тюнинг")').empty?
        item.parent.css('span:contains("Тюнинг")').first.parent.text.sub('Тюнинг:', '').gsub(/\r?\n/, '<br>')
      end
      no_docs = unless item.parent.css('span:contains("Особые отметки")').empty?
        item.parent.css('span:contains("Особые отметки")').first.next_sibling.text.strip.include?("без документов")
      else
        false
      end
      broken = unless item.parent.css('span:contains("Особые отметки")').empty?
        item.parent.css('span:contains("Особые отметки")').first.next_sibling.text.strip.include?("битый или не на ходу")
      else
        false
      end
      sold = doc.css('span.warning strong').text.include?("продан")
      closed = doc.css('span.warning strong').text.include?("Объявление находится в архиве")
      
      seller_email = nil
      seller_source_url  = nil
      seller_city  = nil
      seller_site_url = nil
      seller_address = nil
      # seller_email = unless doc.css('span:contains("E-mail")').empty?
      #   binding.pry
      #   doc.css('span:contains("E-mail")').first.next.next.next.css('a').first.text
      # end
      seller_city = unless doc.css('span:contains("Город")').empty?
        doc.css('span:contains("Город")').first.next_sibling.text.strip
      end
      seller_site_url = unless doc.css('span:contains("Сайт:")').empty?
        doc.css('span:contains("Сайт:")').first.next_sibling.next_sibling.next_sibling.attribute('href').value
      end
      seller_address = unless doc.css('span:contains("Адрес:")').empty?
        doc.css('span:contains("Адрес:")').first.next_sibling.text.strip
      end
      seller_source_url = unless doc.css('a:contains("Посмотреть карточку продавца")').empty?
        doc.css('a:contains("Посмотреть карточку продавца")').first.attribute('href').value
      end
      phone = parse_phone(doc)
      phone.sub!(/\d\+/, ",+") if !phone.nil? && phone.length > 0

      photos = []
      photos_block = doc.css('#usual_photos a')
      if photos_block.empty?
        pic = doc.css('table.auto td img').first.attribute('src').value
        photos << pic
      else
        photos_block.each do |link|
          next if link.css('img').empty?
          pic = []
          pic[0] = link.attribute('href').value
          pic[1] = link.css('img').first.attribute('src').value
          photos << pic
        end
      end
      photos_count = photos.flatten.map{ |v| v if v.include?('/tn_') }.uniq.reject { |c| c.nil? || c.empty? }.size unless photos.empty?
      # binding.pry

      model_rate = unless doc.css('.b-sticker.b-sticker_theme_rating').empty?
        doc.css('.b-sticker.b-sticker_theme_rating').first.child.next.text.to_f
      else
        0.0
      end
      # binding.pry
      brd = $brands_set.filter(title: car[:brand])
      if brd.first.nil?
        $brands_set.insert({
          title: car[:brand],
          created_at: DateTime.now,
          updated_at: DateTime.now,
          slug: car[:brand].downcase.gsub(' ', '_').gsub("'", '')
        })
        brd = $brands_set.filter(title: car[:brand])
      end
      car[:model] = car[:model].sub(equipment_name, '').strip if !equipment_name.nil? && equipment_name != "" && car[:model].include?(equipment_name)
      mdl = $models_set.filter(title: car[:model], brand_id: brd.first[:id])
      if mdl.first.nil?
        $models_set.insert({
          title: car[:model], 
          brand_id: brd.first[:id], 
          rate: model_rate,
          created_at: DateTime.now,
          updated_at: DateTime.now,
          slug: car[:model].downcase.gsub(' ', '_').gsub("'", '')
        })
        mdl = $models_set.filter(title: car[:model], brand_id: brd.first[:id])
      elsif mdl.first[:rate].nil?
        mdl.update({
          rate: model_rate,
          updated_at: DateTime.now
        })
      end
      car = $dataset.filter(id: car[:id])
      car.update(
        seller_address: seller_address,
        seller_site_url: seller_site_url,
        equipment_name: equipment_name,
        equipment_url: equipment_url,
        kms_not_in_rus: kms_not_in_rus,
        body_type: body_type,
        exchange: exchange,
        closed: closed,
        tuning: tuning,
        brand_id: brd.first[:id],
        model_id: mdl.first[:id],
        petrol: petrol, 
        color: color, 
        odometer: kms, 
        new_car: new_car, 
        steer_wheel: steer_wheel, 
        details: details,
        sold: sold,
        photos: photos.join(','),
        photos_count: photos_count,
        phone: phone.to_s.size > 5 ? phone : car.first[:phone],
        seller_email: seller_email.to_s.size > 5 ? seller_email : car.first[:seller_email],
        seller_city:  seller_city,
        seller_source_url: seller_source_url,
        no_docs: no_docs,
        broken:  broken,
        details_parsed_at: DateTime.now
      )
    else
      # binding.pry
      if response.return_code.to_s == "got_nothing"
        puts response.return_code
        req = Typhoeus::Request.new(car[:source_url], $options)
        req.on_complete do |response|
          detailed_scrape(car, response)
        end
        $hydra.queue req
        sleep rand(10..15)
      elsif response.return_code == 200
        binding.pry
        car = $dataset.filter(id: car[:id])
        car.update(
          source_removed: true
        )
      end
    end
    sleep rand(1..100).to_f/100
end 


# -------------------------------------------------------
# Scrape visible data on car's show page
# -------------------------------------------------------
if run_details_scrape
  $dataset.where(sold: false, closed: false, source_removed: false, photos: nil).
          or(sold: false, closed: false, source_removed: false, photos_count: 0).
          reverse_order(:created_at).each do |car|
    req = Typhoeus::Request.new(car[:source_url], $options)
    req.on_complete do |response|
      detailed_scrape(car, response)
    end
    $hydra.queue req
  end
  $hydra.run
end



Capybara.register_driver :poltergeist do |app|
  Capybara::Poltergeist::Driver.new(app, 
    timeout: 180, 
    # js_errors: false,
    phantomjs_options: [
      # '--load-images=false', 
      # "--proxy-auth=#{proxy.username}:#{proxy.password}",
      # "--proxy=91.197.191.65:9090",
      '--disk-cache=false'
      ])
end
include Capybara::DSL

def click_show_buttons(session)
  unless session.html.include?('Внимание! Автомобиль продан,')
    if session.html.include?('Посмотреть карточку продавца')
        if session.html.include?("Показать телефон")
          session.click_button("Показать телефон")
        else
          unless session.all('a[href$="mailto:"]').empty?
            session.find('a[href$="mailto:"]').hover
          end
        end
    else
      unless session.all('#show_contacts > span.b-button__text').empty?
        session.click_button("Показать телефон")
      end
    end
  end
  sleep 2
end


# -------------------------------------------------------
# Parse phone and email with js interactions
# -------------------------------------------------------
if run_email_phone_parse
  $dataset.where(sold: false, source_removed:false, closed: false, phone: "").
          or(sold: false, source_removed:false, closed: false, phone: nil).
          or('sold = false AND source_removed = false AND closed = false AND seller_source_url IS NOT NULL AND seller_email IS NULL').
  # $dataset.where('sold = false AND source_removed = false AND closed = false AND length(phone) = 34').
          reverse_order(:created_at).each do |car|
    # binding.pry
    # car[:source_url] = "http://novosibirsk.drom.ru/ford/mondeo/20087569.html"
    puts "#{car[:id]} : #{car[:source_url]}"
    session = Capybara::Session.new(:poltergeist)
    visit_state = false
    while !visit_state do
      begin
        session.visit car[:source_url] 
        visit_state = true
      rescue Capybara::Poltergeist::TimeoutError => e
        sleep rand(5*60..10*60)
      end
    end
    click_show_buttons(session)
    doc = Nokogiri::HTML(session.html)
    phone = ""
    phone = parse_phone(doc)
    seller_email = nil
    seller_email = unless doc.css('span:contains("E-mail")').empty?
      doc.css('span:contains("E-mail")').first.next.next.next.css('a').first.text
    end
    # binding.pry
    if !phone.nil? && phone.length > 0 && phone.index('+', 5).to_i > 5
      st = phone.index('+', 5)
      fn = st + 1 
      phone[st..fn] = ',+7'
    end
    puts phone
    if !phone.nil? && phone != "" 
      car = $dataset.filter(id: car[:id])
      car.update(
        phone: phone,
        seller_email: seller_email.to_s.length > 5 ? seller_email : car.first[:seller_email]
      )
      puts 'Updated'
      sleep rand(10..20)
    else
      closed = doc.css('span.warning strong').text.include?("Объявление находится в архиве")
      sold = doc.css('span.warning strong').text.include?("продан")
      source_removed = doc.css('.adv-text .b-media-cont.b-media-cont_relative').first ? false : true
      if sold || closed || source_removed
        car = $dataset.filter(id: car[:id])
        car.update(
          sold: sold,
          closed: closed,
          source_removed: source_removed
        )
        puts 'Sold or Closed or Removed'
      elsif doc.css('img#captchaImageContainer').empty?
        puts 'No captcha or Phone'
      else
        captcha_filename = 'captcha.jpeg'
        session.save_screenshot(captcha_filename, :selector => 'img#captchaImageContainer')
        ag_api = AntigateAPIClient.new( ENV['ANITGATE_KEY'] )
        id = ag_api.send_captcha( captcha_filename )
        sleep( 10 ) # recognition_time
        code = nil
        while code == nil do
          code = ag_api.get_captcha_text( id )
          sleep 1
        end
        puts 'captcha: ' + code
        unless code.include?("error_")
          next if doc.css('#captchaInputContainer input').empty?
          input = session.find('#captchaInputContainer input')
          input.set(code.force_encoding('UTF-8').downcase)
          session.click_button('captchaSubmitButton') 
          sleep 5
          doc = Nokogiri::HTML(session.html)
          phone = parse_phone(doc)
          if !phone.nil? && phone.length > 0 && phone.index('+', 5).to_i > 5
            st = phone.index('+', 5)
            fn = st + 1 
            phone[st..fn] = ',+7'
          end
          puts phone
          if !phone.nil? && phone != "" 
            car = $dataset.filter(id: car[:id])
            car.update(
              phone: phone,
              seller_email: seller_email.to_s.length > 5 ? seller_email : car.first[:seller_email]
            )
            puts 'Updated'
          end
          # session.save_screenshot('page.jpeg', :selector => '.adv-text')
        end
      end
    end
    session.driver.quit  
  end
end
