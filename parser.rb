require "rubygems"
require 'open-uri'
require 'net/http'
require 'net/http/post/multipart'
require 'nokogiri'
# require 'watir-webdriver'
require 'capybara'
require 'capybara/dsl'
require 'capybara/poltergeist'
require 'pry'
require 'ap'
# require 'pg'
require "sequel"

rub_table_search = false
rub_details_scrape = false
run_db_migrate_to_rails = false
ARGV.each do|a|
  rub_table_search = true if a.to_i == 1
  rub_details_scrape = true if a.to_i == 2
  run_db_migrate_to_rails = true if a.to_i == 3
end

# DB = Sequel.connect('postgres://user:password@host:port/database_name')
# DB = Sequel.connect('postgres://localhost/cars')
DB = Sequel.connect('postgres://localhost/car_monitor_development')
unless DB.table_exists?(:cars)
  DB.create_table :cars do
    primary_key :id
    Date :date
    String :source_url
    String :preview_url
    String :brand
    String :model
    Integer :year
    Integer :horsepower
    Float :engine_v
    String :transmission
    String :wheel_drive
    Integer :odometer
    Integer :cost
    String :city
    Text :details
    String :steer_wheel
    String :color
    String :petrol
    Boolean :sold, default: false
    DateTime :row_parsed_at
    DateTime :details_parsed_at
    DateTime :created_at
    DateTime :updated_at
    String :phone
    Boolean :new_car, default: false
    String :photos
    String :seller_email
    String :seller_source_url
    String :seller_city
    Boolean :source_removed, default: false
    Boolean :no_docs, default: false
    Boolean :broken, default: false
  end
end
# DB.add_column :cars, :brand, String
# DB.add_column :cars, :broken, :boolean, :default => false
# DB.add_column :cars, :phone, :string
# DB.add_column :items, :name, :text, :unique => true, :null => false
# DB.add_column :items, :category, :text, :default => 'ruby'
# DB[:cars].where(source_removed:nil).update(source_removed:false)
# DB.set_column_default :cars, :source_removed, false
dataset = DB[:cars]
brands_set = DB[:brands]
models_set = DB[:models]
# dataset.filter(year: 2009).map(:id) => [16, 26, 29]
# dataset.filter(id: 8277).delete

Capybara.default_driver = :poltergeist
# Capybara.javascript_driver = :poltergeist
Capybara.run_server = false

# Register PhantomJS (aka poltergeist) as the driver to use
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

# Test proxy
# session = Capybara::Session.new(:poltergeist)
# session.visit "https://switchvpn.net/order"
# session.save_screenshot('page.jpeg')
# session.driver.quit

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
# min_year = 2009
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

def generate_url(region, firms, firm_id, firm_cnt, model_id, model_cnt, min_year, max_year, minprice, maxprice, transmission_id, privod)
  # all cities
  # http://auto.drom.ru/hyundai/coupe/?minyear=2007&transmission=2&order=year&go_search=2
  # Nsk reg
  # http://auto.drom.ru/region54/hyundai/coupe/?minyear=2007&transmission=2&order=year&go_search=2007
  # Nsk 
  # http://novosibirsk.drom.ru/hyundai/coupe/?minyear=2007&transmission=2&order=year&go_search=2007

  # "http://#{domain}/#{region}#{firm_id}/#{model_id[model_cnt]}#{page}/?go_search=2&minyear=#{min_year}&transmission=#{transmission_id}&privod=#{privod}&order=year"
  url = 'http://'
  url += if region
    'auto.drom.ru/region54/'
  elsif city
    'novosibirsk.drom.ru/'
  else
    'auto.drom.ru/'
  end

  url += if firm_id
    firm_id + '/'
  end

  url += if model_id && !model_id.empty?
    model_id[model_cnt] + '/'
  else
    ''
  end

  url += "?go_search=2"

  url += if min_year
    "&minyear=#{min_year}"
  end

  url += if max_year
    "&maxyear=#{max_year}"
  end

  url += if minprice
    "&minprice=#{minprice}"
  end

  url += if maxprice
    "&maxprice=#{maxprice}"
  end

  url += if transmission_id
    "&transmission=#{transmission_id}"
  end

  url += if privod
    "&privod=#{privod}"
  end

  url += "&order=year"
end

# Start up a new thread
# session = Capybara::Session.new(:poltergeist)
# Report using a particular user agent
# session.driver.headers = { 'User-Agent' => "Mozilla/5.0 (Macintosh; Intel Mac OS X)" }

def scrape_table_row(item, firms, firm_id, model_id, brands_set, models_set)
  date = Date.strptime( item.css('td:nth-child(1) a').text , '%d-%m')
  link = item.css('td:nth-child(2) img').first.parent.attribute('href').value
  preview = item.css('td:nth-child(2) img').attribute('src').value
  model = item.css('td:nth-child(3)').text.strip.squeeze(' ')
  equipment_name = item.css('td:nth-child(3) small').text.strip.squeeze(' ')
  model = model.sub(equipment_name, '').strip if equipment_name.length > 0
  sold = item.css('td:nth-child(3) strike').text.size > 0 ? true : false

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

  brd = brands_set.filter(title: brand)
  if brd.first.nil?
    brands_set.insert({
      title: brand,
      created_at: DateTime.now,
      updated_at: DateTime.now
    })
    brd = brands_set.filter(title: brand)
  end
  # binding.pry
  mdl = models_set.filter(title: model, brand_id: brd.first[:id])
  if mdl.first.nil?
    models_set.insert({
      title: model, 
      brand_id: brd.first[:id], 
      # rate: model_rate,
      created_at: DateTime.now,
      updated_at: DateTime.now
    })
    mdl = models_set.filter(title: model, brand_id: brd.first[:id])
  # elsif mdl.first[:rate].nil?
  #   mdl.update({
  #     rate: model_rate,
  #     updated_at: DateTime.now
  #   })
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
    row_parsed_at: DateTime.now
  }
end

if rub_table_search
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
      next if maxprice == minprice
      (beg_year..end_year).step( year_step ) do |year|
        min_year = year
        max_year = year + year_step
        max_year = Date.today.year if max_year > Date.today.year
        next if max_year == min_year
        url = generate_url(region, firms, firm_id, firm_cnt, model_id, model_cnt, min_year, max_year, minprice, maxprice, transmission_id, privod)
        loop do
          cnt = 0
          upd_cnt = 0
          print url
          # sleep rand(5..20)
          session = Capybara::Session.new(:poltergeist)
          session.visit url 
          session.save_screenshot('page.jpeg')
          doc = Nokogiri::HTML(session.html)
          # doc = Nokogiri::HTML(open(url))
          unless doc.css('.subscriptions_link_wrapper').empty?
            doc.css('.subscriptions_link_wrapper').first.parent.css('tr.row').each do |item|
                current_car = scrape_table_row(item, firms, firm_id, model_id, brands_set, models_set)
                # cars << current_car
                car = dataset.where(source_url: current_car[:source_url]).first
                unless car
                  dataset.insert(current_car)
                  cnt += 1
                else
                  if car[:sold] != current_car[:sold]
                    # print " '#{ car[:id] }' "
                    car = dataset.filter(id: car[:id])
                    car.update(
                      sold: current_car[:sold]
                    )
                    upd_cnt += 1
                  end
                end
            end
            doc.css('.subscriptions_link_wrapper').first.parent.css('tr.h').each do |item|
                current_car = scrape_table_row(item, firms, firm_id, model_id, brands_set, models_set)
                # cars << current_car
                car = dataset.where(source_url: current_car[:source_url]).first
                unless car
                  dataset.insert(current_car)
                  cnt += 1
                else
                  if car[:sold] != current_car[:sold]
                    car = dataset.filter(id: car[:id])
                    car.update(
                      sold: current_car[:sold]
                    )
                    upd_cnt += 1
                  end
                end
            end
          end
          print " : #{cnt} : #{upd_cnt}\n"
          pager = doc.css('.pager')
          # binding.pry
          if pager && (pager.css('> a').text == "Следующая" || pager.css('> a:last').text == "Следующая" )
            page = pager.css(' > a:last').attribute('href').value
            url = page
          elsif model_id && model_cnt < ((model_id.size) - 1)
            model_cnt += 1
            url = generate_url(region, firms, firm_id, firm_cnt, model_id, model_cnt, min_year, max_year, minprice, maxprice, transmission_id, privod)
          else
            session.driver.quit
            break
          end
          session.driver.quit
        end
      end
    end

  end
end



def send_captcha( key, captcha_file )
  uri = URI.parse( 'http://antigate.com/in.php' )
  file = File.new( captcha_file, 'rb' )
  req = Net::HTTP::Post::Multipart.new( uri.path,
                                        :method => 'post',
                                        :key => key,
                                        :file => UploadIO.new( file, 'image/jpeg', 'image.jpg' ),
                                        :is_russian => 1)
  http = Net::HTTP.new( uri.host, uri.port )
  begin
    resp = http.request( req )
  rescue => err
    puts err
    return nil
  end#begin
  
  id = resp.body
  return id[ 3..id.size ]
end#def

def get_captcha_text( key, id )
  data = { :key => key,
           :action => 'get',
           :id => id,
           :min_len => 5,
           :max_len => 5 }
  uri = URI.parse('http://antigate.com/res.php' )
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
  end#if
  return nil
end#def

def report_bad( key, id )
  data = { :key => key,
           :action => 'reportbad',
           :id => id }
  uri = URI.parse('http://antigate.com/res.php' )
  req = Net::HTTP::Post.new( uri.path )
  http = Net::HTTP.new( uri.host, uri.port )
  req.set_form_data( data )

  begin
    resp = http.request(req)
  rescue => err
    puts err
  end
end#def



def parse_phone(session)
  doc = Nokogiri::HTML(session.html)
  phone = if session.html.include?('Посмотреть карточку продавца')
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

# off = 1058 - dataset.order(:id).first[:id] 
# off = 400
# dataset.offset(off).each do |car|
# dataset.filter('id > 1749').each do |car|
if rub_details_scrape
  # dataset.where('updated_at < ? AND sold = false AND source_removed = false', Time.now - 12*60*60).each do |car|
  # dataset.where('created_at < ? AND updated_at < ? AND sold = false AND source_removed = false', Time.now - 12*60*60).each do |car|
  # dataset.where('id > 15461 AND sold = false AND source_removed = false').each do |car|
  # dataset.where('created_at < ? AND sold = false AND source_removed = false', Time.now - 12*60*60 ).reverse_order(:created_at).each do |car|
  dataset.where('sold = false AND source_removed = false').where(photos: nil).reverse_order(:created_at).each do |car|
  # dataset.where("year = 2015 AND seller_source_url IS NOT NULL AND equipment_name IS NOT NULL").reverse_order(:created_at).each do |car|
  # dataset.where(id:[23321, 23320, 18317, 8899]).reverse_order(:created_at).each do |car|
  # dataset.where(id:13815).reverse_order(:created_at).each do |car|
    # next if car[:created_at] != car[:updated_at]
    puts "#{car[:id]} : #{car[:source_url]}"
    session = Capybara::Session.new(:poltergeist)
    session.visit car[:source_url]
    unless session.html.include?('Внимание! Автомобиль продан,')
      if session.html.include?('Посмотреть карточку продавца')
          if session.html.include?("Показать телефон")
            session.click_button("Показать телефон")
          else
            unless session.all('a[href$="mailto:"]').empty?
              session.find('a[href$="mailto:"]').hover
              sleep 2
            end
          end
      else
        # binding.pry
        unless session.all('#show_contacts > span.b-button__text').empty?
          session.click_button("Показать телефон")
        end
      end
    end
    doc = Nokogiri::HTML(session.html)
    # binding.pry
    # doc = Nokogiri::HTML(open(car[:link]))
    item = doc.css('.adv-text .b-media-cont.b-media-cont_relative').first
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
      seller_email = unless doc.css('span:contains("E-mail")').empty?
        doc.css('span:contains("E-mail")').first.next.next.next.css('a').first.text
      end
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
      phone = parse_phone(session)
      phone.sub!(/\d\+/, ",+") if !phone.nil? && phone.length > 0
      if (phone.nil? || phone == "") && !doc.css('img#captchaImageContainer').empty?
        session.save_screenshot('page.jpeg', :selector => '.adv-text')
        session.save_screenshot('captcha.jpeg', :selector => 'img#captchaImageContainer')
        key = ENV['ANITGATE_KEY']
        captcha = 'captcha.jpeg'
        recognition_time = 10
        #recognize capcha
        id = send_captcha( key, captcha )
        sleep( recognition_time )
        code = nil
        while code == nil do
          code = get_captcha_text( key, id )
          sleep 1
        end#while
        puts 'captcha: ' + code
        unless code.include?("error_")
          input = session.find('#captchaInputContainer input')
          input.set(code.force_encoding('UTF-8').downcase)
          session.click_button('captchaSubmitButton') 
          sleep rand(5..10)
          phone = parse_phone(session)
          phone.sub!(/\d\+/, ",+") if !phone.nil? && phone.length > 0
          puts phone
        end
      end
      # binding.pry

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

      model_rate = unless doc.css('.b-sticker.b-sticker_theme_rating').empty?
        doc.css('.b-sticker.b-sticker_theme_rating').first.child.next.text.to_f
      else
        0.0
      end
      brd = brands_set.filter(title: car[:brand])
      if brd.first.nil?
        brands_set.insert({
          title: car[:brand],
          created_at: DateTime.now,
          updated_at: DateTime.now
        })
        brd = brands_set.filter(title: car[:brand])
      end
      car[:model] = car[:model].sub(equipment_name, '').strip if !equipment_name.nil? && equipment_name != "" && car[:model].include?(equipment_name)
      mdl = models_set.filter(title: car[:model], brand_id: brd.first[:id])
      if mdl.first.nil?
        models_set.insert({
          title: car[:model], 
          brand_id: brd.first[:id], 
          rate: model_rate,
          created_at: DateTime.now,
          updated_at: DateTime.now
        })
        mdl = models_set.filter(title: car[:model], brand_id: brd.first[:id])
      elsif mdl.first[:rate].nil?
        mdl.update({
          rate: model_rate,
          updated_at: DateTime.now
        })
      end

      car = dataset.filter(id: car[:id])
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
        phone: phone,
        seller_email: seller_email,
        seller_city:  seller_city,
        seller_source_url:  seller_source_url,
        no_docs: no_docs,
        broken:  broken,
        details_parsed_at: DateTime.now
      )

      # binding.pry
    else
      car = dataset.filter(id: car[:id])
      car.update(
        source_removed: true
      )
    end
    session.driver.quit
  end
end
# binding.pry

if run_db_migrate_to_rails
  DB_RAILS = Sequel.connect('postgres://localhost/car_monitor_development')
  # dataset = DB[:cars]
  dataset_rails = DB_RAILS[:cars]
  dataset.each do |car|
    current_car = {
      date: car[:date],
      source_url: car[:link],
      preview_url: car[:preview],
      model: car[:model],
      brand: car[:brand],
      year: car[:year],
      horsepower: car[:horsepower],
      engine_v: car[:engine_v],
      transmission: car[:transmission],
      wheel_drive: car[:wir_dr],
      odometer: car[:kms],
      cost: car[:cost],
      city: car[:city],
      sold: car[:sold],
      no_docs: car[:no_docs],
      broken: car[:broken],
      petrol: car[:petrol], 
      color: car[:color], 
      odometer: car[:kms], 
      new_car: car[:new_car], 
      steer_wheel: car[:steer_wheel], 
      details: car[:details],
      sold: car[:sold],
      photos: car[:photos],
      phone: car[:phone],
      seller_email: car[:seller_email],
      seller_city:  car[:seller_city],
      seller_source_url:  car[:seller_source_url],
      no_docs: car[:no_docs],
      broken:  car[:broken],
      row_parsed_at: car[:created_at],
      details_parsed_at: car[:updated_at],
      created_at: car[:created_at],
      updated_at: car[:updated_at]
    }
    car2 = dataset_rails.where(source_url: car[:link]).first
    # binding.pry
    if car2.nil? 
      dataset_rails.insert(current_car)
    else
      car2 = dataset_rails.filter(id: car2[:id])
      car2.update(current_car)
    end
  end
end

puts dataset.count
# puts cars.count
# ap cars.first
# ap cars.last




