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
  end
end
dataset = DB[:cars]


Capybara.default_driver = :poltergeist
# Capybara.javascript_driver = :poltergeist
Capybara.run_server = false

# Register PhantomJS (aka poltergeist) as the driver to use
Capybara.register_driver :poltergeist do |app|
  Capybara::Poltergeist::Driver.new(app, phantomjs_options: ['--load-images=false', '--disk-cache=false'])
end


# all cities
# http://auto.drom.ru/hyundai/coupe/?minyear=2007&transmission=2&order=year&go_search=2
# Nsk reg
# http://auto.drom.ru/region54/hyundai/coupe/?minyear=2007&transmission=2&order=year&go_search=2007
# Nsk 
# http://novosibirsk.drom.ru/hyundai/coupe/?minyear=2007&transmission=2&order=year&go_search=2007

include Capybara::DSL

domain = "auto.drom.ru"
region = '' 
# region = 'region54/'
# firm_id = 'hyundai'
# model_id = %w(coupe tiburon tuscani)
# model_id = %w(coupe)
# min_year = 2008
firm_id = 'toyota'
model_id = %w(celica)
transmission_id = 2 # autoamatic
# privod = 2 # 1 - передний, 2 - задний, 3 - 4WD 
privod ||= 0
min_year ||= 0 
page = ''

cars = []
current_car = Hash.new
model_cnt = 0

# Start up a new thread
session = Capybara::Session.new(:poltergeist)
# Report using a particular user agent
session.driver.headers = { 'User-Agent' => "Mozilla/5.0 (Macintosh; Intel Mac OS X)" }

def scrape_table_row(item)
  print '.' 

  date = Date.strptime( item.css('td:nth-child(1) a').text , '%d-%m')
  link = item.css('td:nth-child(2) img').first.parent.attribute('href').value
  preview = item.css('td:nth-child(2) img').attribute('src').value
  model = item.css('td:nth-child(3)').text.strip.squeeze(' ')
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

# url = "http://auto.drom.ru/#{region}#{firm_id}/#{model_id[0]}#{page}/?go_search=2&minyear=#{min_year}&transmission=#{transmission_id}&order=year"
url = "http://#{domain}/#{region}#{firm_id}/#{model_id[model_cnt]}#{page}/?go_search=2&minyear=#{min_year}&transmission=#{transmission_id}&privod=#{privod}&order=year"
loop do
  session.visit url 
  doc = Nokogiri::HTML(session.html)
  # doc = Nokogiri::HTML(open(url))
  unless doc.css('.subscriptions_link_wrapper').empty?
    doc.css('.subscriptions_link_wrapper').first.parent.css('tr.row').each do |item|
        current_car = scrape_table_row(item)
        # cars << current_car
        unless dataset.where(link: current_car[:link]).first
          dataset.insert(current_car)
        end
    end
    doc.css('.subscriptions_link_wrapper').first.parent.css('tr.h').each do |item|
        current_car = scrape_table_row(item)
        # cars << current_car
        unless dataset.where(link: current_car[:link]).first
          dataset.insert(current_car)
        end
    end
  end
  pager = doc.css('.pager')
  # binding.pry
  if pager && (pager.css('> a').text == "Следующая" || pager.css('> a:last').text == "Следующая" )
    page = pager.css(' > a').attribute('href').value
    url = page
  elsif model_cnt < model_id.size - 1
    model_cnt += 1
    url = "http://#{domain}/#{region}#{firm_id}/#{model_id[model_cnt]}#{page}/?go_search=2&minyear=#{min_year}&transmission=#{transmission_id}&privod=#{privod}&order=year"
  else
    break
  end
end

# cars.each do |car|
#   puts car[:link]
#   session.visit car[:link]
#   unless session.html.include?('Внимание! Автомобиль продан,')
#     session.click_button("Показать телефон")
#   end
#   doc = Nokogiri::HTML(session.html)
#   # binding.pry
#   # doc = Nokogiri::HTML(open(car[:link]))
#   item = doc.css('.adv-text .b-media-cont.b-media-cont_relative').first
#   car[:petrol] = unless item.css('span:contains("Двигатель")').empty?
#     item.css('span:contains("Двигатель")').first.next_sibling.text.strip.split(",")[0]
#   end
#   car[:color] = unless item.css('span:contains("Цвет")').empty?
#     item.css('span:contains("Цвет")').first.next_sibling.text.strip
#   end
#   car[:kms] = unless item.css('span:contains("Пробег, км")').empty?
#     item.css('span:contains("Пробег, км")').first.next_sibling.text.strip.to_i
#   end
#   car[:new_car] = unless item.css('span:contains("Пробег")').empty?
#     item.css('span:contains("Пробег")').first.next_sibling.text.include?("Новый")
#   end
#   car[:steer_wheel] = unless item.css('span:contains("Руль")').empty?
#     item.css('span:contains("Руль")').first.next_sibling.text.strip
#   end
#   car[:details] = unless item.parent.css('span:contains("Дополнительно")').empty?
#     item.parent.css('span:contains("Дополнительно")').first.parent.text.sub('Дополнительно:', '').gsub(/\r?\n/, '<br>')
#   end
#   car[:phone] = doc.css('.b-media-cont__label.b-media-cont__label_no-wrap').text
#   car[:sold] = doc.css('span.warning strong').text.include?("продан")
#   car[:photos] = []
#   doc.css('#usual_photos img').each do |img|
#     car[:photos] << img.attribute('src').value
#   end
# end

# binding.pry

puts dataset.count
# puts cars.count
# ap cars.first
# ap cars.last




