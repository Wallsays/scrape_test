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

rub_table_search = false
rub_details_scrape = false
ARGV.each do|a|
  rub_table_search = true if a.to_i == 1
  rub_details_scrape = true if a.to_i == 2
end

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
    Boolean :sold, default: false
    DateTime :created_at
    DateTime :updated_at
    String :phone
    Boolean :new_car, default: false
    String :photos
    String :seller_email
    String :seller_link
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
# dataset.filter(year: 2009).map(:id) => [16, 26, 29]
# dataset.filter(id: 8277).delete

Capybara.default_driver = :poltergeist
# Capybara.javascript_driver = :poltergeist
Capybara.run_server = false

# Register PhantomJS (aka poltergeist) as the driver to use
Capybara.register_driver :poltergeist do |app|
  Capybara::Poltergeist::Driver.new(app, 
    timeout: 120, 
    # js_errors: false,
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
  'renault' => [],
  'skoda' => [],
  'opel' => [],
  'peugeot' => [],
  'land_rover' => [],
  'ssang_yong' => [],
  'citroen' => []
}
min_year = 2000
max_year = 2015
minprice = 200000
maxprice = 1000000
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

def scrape_table_row(item, firms, firm_id, model_id)
  date = Date.strptime( item.css('td:nth-child(1) a').text , '%d-%m')
  link = item.css('td:nth-child(2) img').first.parent.attribute('href').value
  preview = item.css('td:nth-child(2) img').attribute('src').value
  model = item.css('td:nth-child(3)').text.strip.squeeze(' ')
  sold = item.css('td:nth-child(3) strike').text.size > 0 ? true : false

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
    sold: sold,
    no_docs: no_docs,
    broken: broken,
    created_at: DateTime.now,
    updated_at: DateTime.now
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
      (beg_year..end_year).step( year_step ) do |year|
        min_year = year
        max_year = year + year_step
        max_year = Date.today.year if max_year > Date.today.year
        url = generate_url(region, firms, firm_id, firm_cnt, model_id, model_cnt, min_year, max_year, minprice, maxprice, transmission_id, privod)
        loop do
          cnt = 0
          print url
          session = Capybara::Session.new(:poltergeist)
          session.visit url 
          doc = Nokogiri::HTML(session.html)
          # doc = Nokogiri::HTML(open(url))
          unless doc.css('.subscriptions_link_wrapper').empty?
            doc.css('.subscriptions_link_wrapper').first.parent.css('tr.row').each do |item|
                current_car = scrape_table_row(item, firms, firm_id, model_id)
                # cars << current_car
                unless dataset.where(link: current_car[:link]).first
                  dataset.insert(current_car)
                  cnt += 1
                end
            end
            doc.css('.subscriptions_link_wrapper').first.parent.css('tr.h').each do |item|
                current_car = scrape_table_row(item, firms, firm_id, model_id)
                # cars << current_car
                unless dataset.where(link: current_car[:link]).first
                  dataset.insert(current_car)
                  cnt += 1
                end
            end
          end
          print " : #{cnt}\n"
          pager = doc.css('.pager')
          # binding.pry
          if pager && (pager.css('> a').text == "Следующая" || pager.css('> a:last').text == "Следующая" )
            page = pager.css(' > a:last').attribute('href').value
            url = page
          elsif model_id && model_cnt < ((model_id.size) - 1)
            model_cnt += 1
            url = generate_url(region, firms, firm_id, firm_cnt, model_id, model_cnt, min_year, max_year, minprice, maxprice, transmission_id, privod)
          else
            break
          end
          session.driver.quit
        end
      end
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
  dataset.where('sold = false AND source_removed = false').where(photos:nil).reverse_order(:created_at).each do |car|
    # next if car[:created_at] != car[:updated_at]
    puts "#{car[:id]} : #{car[:link]}"
    session = Capybara::Session.new(:poltergeist)
    session.visit car[:link]
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
      new_car = unless item.css('span:contains("Пробег")').empty?
        item.css('span:contains("Пробег")').first.next_sibling.text.include?("Новый")
      end
      steer_wheel = unless item.css('span:contains("Руль")').empty?
        item.css('span:contains("Руль")').first.next_sibling.text.strip
      end
      details = unless item.parent.css('span:contains("Дополнительно")').empty?
        item.parent.css('span:contains("Дополнительно")').first.parent.text.sub('Дополнительно:', '').gsub(/\r?\n/, '<br>')
      end
      no_docs = unless item.parent.css('span:contains("Особые отметки")').empty?
        item.parent.css('span:contains("Особые отметки")').first.next_sibling.text.strip.include?("без документов")
      end
      broken = unless item.parent.css('span:contains("Особые отметки")').empty?
        item.parent.css('span:contains("Особые отметки")').first.next_sibling.text.strip.include?("битый или не на ходу")
      end
      sold = doc.css('span.warning strong').text.include?("продан")
      
      seller_email = nil
      seller_link  = nil
      seller_city  = nil
      phone = if session.html.include?('Посмотреть карточку продавца')
        seller_email = unless doc.css('span:contains("E-mail")').empty?
          doc.css('span:contains("E-mail")').first.next.next.next.css('a').first.text
        end
        seller_city = unless doc.css('span:contains("Город")').empty?
          doc.css('span:contains("Город")').first.next_sibling.text.strip
        end
        seller_link = unless doc.css('a:contains("Посмотреть карточку продавца")').empty?
          doc.css('a:contains("Посмотреть карточку продавца")').first.attribute('href').value
        end
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
        doc.css('.b-media-cont__label.b-media-cont__label_no-wrap').text
      end
      
      # if session.html.include?('Посмотреть карточку продавца') && !sold && (phone.nil? || phone.empty?)
      #   puts phone
      #   puts seller_email
      #   binding.pry
      # end

      photos = []
      doc.css('#usual_photos a').each do |link|
        next if link.css('img').empty?
        pic = []
        pic[0] = link.attribute('href').value
        pic[1] = link.css('img').first.attribute('src').value
        photos << pic
      end

      car = dataset.filter(id: car[:id])
      car.update(
        petrol: petrol, 
        color: color, 
        kms: kms, 
        new_car: new_car, 
        steer_wheel: steer_wheel, 
        details: details,
        sold: sold,
        photos: photos.join(','),
        phone: phone,
        seller_email: seller_email,
        seller_city:  seller_city,
        seller_link:  seller_link,
        no_docs: no_docs,
        broken:  broken,
        updated_at: DateTime.now
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

puts dataset.count
# puts cars.count
# ap cars.first
# ap cars.last




