require "rubygems"
require 'open-uri'
require 'nokogiri'
# require 'watir-webdriver'
require 'capybara'
require 'capybara/dsl'
require 'capybara/poltergeist'
require 'pry'
require 'ap'
# require 'pg'
require "sequel"


# DB = Sequel.connect('postgres://user:password@host:port/database_name')
DB = Sequel.connect('postgres://localhost/cars')
unless DB.table_exists?(:cars)
  DB.create_table :cars do
    primary_key :id
    String :column_name 
    Date :date
    String :link
    String :preview
    String :brand
    String :model
    Integer :year
    Integer :horsepower
    Float :engine_v
    String :transmission
    String :wir_dr
    Integer :kms
    Integer :cost
    String :city
    Text :details
    String :steer_wheel
    String :color
    String :petrol
    Boolean :sold
    DateTime :created_at
    DateTime :updated_at
    String :phone
    Boolean :new_car
  end
end
# DB.add_column :cars, :brand, String
# DB.add_column :cars, :new_car, :boolean
# DB.add_column :cars, :phone, :string
# DB.add_column :items, :name, :text, :unique => true, :null => false
# DB.add_column :items, :category, :text, :default => 'ruby'
dataset = DB[:cars]
# dataset.filter(year: 2009).map(:id) => [16, 26, 29]
# dataset.filter(id: 8277).delete

Capybara.default_driver = :poltergeist
# Capybara.javascript_driver = :poltergeist
Capybara.run_server = false

# Register PhantomJS (aka poltergeist) as the driver to use
Capybara.register_driver :poltergeist do |app|
  Capybara::Poltergeist::Driver.new(app, 
    timeout: 60, 
    js_errors: false,
    phantomjs_options: [
      '--load-images=false', 
      # "--proxy-auth=#{proxy.username}:#{proxy.password}",
      # "--proxy=92.110.173.139:80",
      '--disk-cache=false'
      ])
end

include Capybara::DSL

domain = "auto.drom.ru"
# city = '' 
# region = '' 
region = 'region54'
# firm_id = 'toyota'
# firm_id = 'mazda'
firm_id = 'honda'
firm_id = 'nissan'
firm_id = 'mitsubishi'
firm_id = 'ford'
firm_id = 'volkswagen'
firm_id = 'mercedes-benz'
firm_id = 'chevrolet'
firm_id = 'hyundai'
firm_id = 'kia'
# model_id = %w(coupe tiburon tuscani)
# model_id = %w(coupe)
# model_id = %w(celica)
min_year = 2000
transmission_id = 2 # autoamatic
minprice = 300000
maxprice = 1000000
# privod = 2 # 1 - передний, 2 - задний, 3 - 4WD 
model_id ||= nil
minprice ||= 0
maxprice ||= 0
privod ||= 0
min_year ||= 0 
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
  "Mazda", "Mercedes-Benz", "Mini", "Mitsubishi",
  "Nissan", "Opel", "Peugeot", "Plymouth", "Pontiac", "Porsche", "Proton",
  "Renault", "Rover", 
  "Saab", "Saturn", "Scion", "SEAT", "Skoda", "Smart", "SsangYong", "Subaru", "Suzuki", 
  "Tianye", "Toyota", "Volkswagen", "Volvo"
]

cars = []
current_car = Hash.new
firm_cnt = 0
model_cnt = 0

def generate_url(region, firm_id, firm_cnt, model_id, model_cnt, min_year, minprice, maxprice, transmission_id, privod)
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

  url += if model_id
    model_id[model_cnt] + '/'
  else
    ''
  end

  url += "?go_search=2&minyear=#{min_year}&minprice=#{minprice}&maxprice=#{maxprice}&transmission=#{transmission_id}&privod=#{privod}&order=year"
end

# Start up a new thread
session = Capybara::Session.new(:poltergeist)
# Report using a particular user agent
session.driver.headers = { 'User-Agent' => "Mozilla/5.0 (Macintosh; Intel Mac OS X)" }

def scrape_table_row(item, firm_id, model_id)
  # print '.' 
  date = Date.strptime( item.css('td:nth-child(1) a').text , '%d-%m')
  link = item.css('td:nth-child(2) img').first.parent.attribute('href').value
  preview = item.css('td:nth-child(2) img').attribute('src').value
  model = item.css('td:nth-child(3)').text.strip.squeeze(' ')

  if model_id && model_id != ''
    brand = model.split(" ").first
  else
    brand = firm_id.capitalize
  end
       
  BRAND_LIST.any? do |word| 
    if model.include?(word)
      brand = word
      model = model.sub(brand,'').strip
    end
  end
  
  year = item.css('td:nth-child(4)').text.delete(' ').to_i
  hp = item.css('td:nth-child(5) .gray').text
  info = item.css('td:nth-child(5)').text.strip.squeeze(' ').sub(hp,'')
  volume = info.include?('л') ? info[0..4].to_f : 0.0
  transmission = info.include?("автомат") ? "автомат" : info.include?("механика") ? "механика" : ""
  wir_dr = info.include?("4WD") ? "4WD" : info.include?("передний") ? "передний" : info.include?("задний") ? "задний" : ""

  kms = item.css('td:nth-child(6)').text.delete(' ').to_i
  cost = item.css('td:nth-child(8) .f14').text.delete(' ').to_i
  city = item.css('td:nth-child(8) span:last').text.delete(' ')
  current_car = {
    date: date,
    link: link,
    preview: preview,
    model: model,
    brand: brand,
    year: year,
    horsepower: hp.delete('(').to_i,
    engine_v: volume,
    transmission: transmission,
    wir_dr: wir_dr,
    kms: kms*1000,
    cost: cost,
    city: city,
    created_at: DateTime.now,
    updated_at: DateTime.now
  }
end

if false
  url = generate_url(region, firm_id, firm_cnt, model_id, model_cnt, min_year, minprice, maxprice, transmission_id, privod)
  loop do
    puts url
    session.visit url 
    doc = Nokogiri::HTML(session.html)
    # doc = Nokogiri::HTML(open(url))
    unless doc.css('.subscriptions_link_wrapper').empty?
      doc.css('.subscriptions_link_wrapper').first.parent.css('tr.row').each do |item|
          current_car = scrape_table_row(item, firm_id, model_id)
          # cars << current_car
          unless dataset.where(link: current_car[:link]).first
            dataset.insert(current_car)
          end
      end
      doc.css('.subscriptions_link_wrapper').first.parent.css('tr.h').each do |item|
          current_car = scrape_table_row(item, firm_id, model_id)
          # cars << current_car
          unless dataset.where(link: current_car[:link]).first
            dataset.insert(current_car)
          end
      end
    end
    pager = doc.css('.pager')
    # binding.pry
    if pager && (pager.css('> a').text == "Следующая" || pager.css('> a:last').text == "Следующая" )
      page = pager.css(' > a:last').attribute('href').value
      url = page
    elsif model_id && model_cnt < ((model_id.size) - 1)
      model_cnt += 1
      url = generate_url(region, firm_id, firm_cnt, model_id, model_cnt, min_year, minprice, maxprice, transmission_id, privod)
    else
      break
    end
  end
end


# off = 1058 - dataset.order(:id).first[:id] 
# off = 400
# dataset.offset(off).each do |car|
# dataset.filter('id > 1749').each do |car|
dataset.where('updated_at < ?', Time.now - 3*60*60).each do |car| # 3 hours
  puts "#{car[:id]} : #{car[:link]}"
  session.visit car[:link]
  # unless session.html.include?('Внимание! Автомобиль продан,')
  #   session.click_button("Показать телефон")
  # end
  doc = Nokogiri::HTML(session.html)
  # binding.pry
  # doc = Nokogiri::HTML(open(car[:link]))
  item = doc.css('.adv-text .b-media-cont.b-media-cont_relative').first
  petrol = unless item.css('span:contains("Двигатель")').empty?
    item.css('span:contains("Двигатель")').first.next_sibling.text.strip.split(",")[0]
  end
  color = unless item.css('span:contains("Цвет")').empty?
    item.css('span:contains("Цвет")').first.next_sibling.text.strip
  end
  kms = unless item.css('span:contains("Пробег, км")').empty?
    item.css('span:contains("Пробег, км")').first.next_sibling.text.strip.to_i
  end
  new_car = unless item.css('span:contains("Пробег")').empty?
    item.css('span:contains("Пробег")').first.next_sibling.text.include?("Новый")
  end
  steer_wheel = unless item.css('span:contains("Руль")').empty?
    item.css('span:contains("Руль")').first.next_sibling.text.strip
  end
  details = unless item.parent.css('span:contains("Дополнительно")').empty?
    item.parent.css('span:contains("Дополнительно")').first.parent.text.sub('Дополнительно:', '').gsub(/\r?\n/, '<br>')
  end
  phone = doc.css('.b-media-cont__label.b-media-cont__label_no-wrap').text
  sold = doc.css('span.warning strong').text.include?("продан")

  # car[:photos] = []
  # doc.css('#usual_photos img').each do |img|
  #   car[:photos] << img.attribute('src').value
  # end
  car = dataset.filter(id: car[:id])
  car.update(
    petrol: petrol, 
    color: color, 
    kms: kms, 
    new_car: new_car, 
    steer_wheel: steer_wheel, 
    details: details,
    phone: phone,
    sold: sold,
    updated_at: DateTime.now
  )

  # binding.pry
end

# binding.pry

puts dataset.count
# puts cars.count
# ap cars.first
# ap cars.last




