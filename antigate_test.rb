# encoding: utf-8

require 'rest-client'
require 'capybara'
require 'capybara/dsl'
require 'capybara/poltergeist'
require 'pry'
require 'ap'
require "sequel"

require 'net/http'
require 'net/http/post/multipart'

# Register PhantomJS (aka poltergeist) as the driver to use
Capybara.register_driver :poltergeist do |app|
  Capybara::Poltergeist::Driver.new(app, 
    timeout: 180, 
    # js_errors: false,
    phantomjs_options: [
      # '--load-images=false', 
      # "--proxy-auth=#{proxy.username}:#{proxy.password}",
      # "--proxy=92.110.173.139:80",
      '--disk-cache=false'
      ])
end

include Capybara::DSL

# Test proxy
# session = Capybara::Session.new(:poltergeist)
# session.visit "https://switchvpn.net/order"
# session.save_screenshot('page.jpeg')
# session.driver.quit

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

DB = Sequel.connect('postgres://localhost/car_monitor_development')
dataset = DB[:cars]
dataset.where('sold = false AND source_removed = false AND closed = false').where(phone: "").reverse_order(:created_at).each do |car|
  puts "#{car[:id]} : #{car[:source_url]}"
  # url = "http://spb.drom.ru/audi/rs7/19807562.html"
  # url = "http://spb.drom.ru/nissan/patrol/20202232.html"
  url = car[:source_url]
  session = Capybara::Session.new(:poltergeist)
  session.visit url 
  unless session.html.include?('Внимание! Автомобиль продан,')
    if session.html.include?('Посмотреть карточку продавца')
        if session.html.include?("Показать телефон")
          session.click_button("Показать телефон")
        end
    else
      unless session.all('#show_contacts > span.b-button__text').empty?
        session.click_button("Показать телефон")
      end
    end
  end
  sleep 2
  phone = ""
  phone = parse_phone(session)
  phone.sub!(/\d\+/, ",+") if !phone.nil? && phone.length > 0
  puts phone
  # binding.pry
  if !phone.nil? && phone != "" 
    car = dataset.filter(id: car[:id])
    car.update(
      phone: phone
    )
    session.driver.quit  
    puts 'Updated'
    sleep rand(10..20)
    next
  end
  doc = Nokogiri::HTML(session.html)
  closed = doc.css('span.warning strong').text.include?("Объявление находится в архиве")
  sold = doc.css('span.warning strong').text.include?("продан")
  source_removed = doc.css('.adv-text .b-media-cont.b-media-cont_relative').first ? false : true
  if sold || closed || source_removed
    # binding.pry
    car = dataset.filter(id: car[:id])
    car.update(
      sold: sold,
      closed: closed,
      source_removed: source_removed
    )
    session.driver.quit  
    puts 'Sold or Closed or Removed'
    next
  end
  if doc.css('img#captchaImageContainer').empty?
    session.driver.quit  
    puts 'No captcha or Phone'
    next
  end
  # binding.pry
  session.save_screenshot('captcha.jpeg', :selector => 'img#captchaImageContainer')
  # response = RestClient.post 'http://anti-captcha.com/in.php',
  #                  :key => ENV['ANITGATE_KEY'], 
  #                  :file => File.new("captcha.jpeg", 'rb')
  # response = RestClient.get 'http://anti-captcha.com/res.php',
  #                  :key => ENV['ANITGATE_KEY'], 
  #                  :action => 'get',
  #                  :file => File.new("captcha.jpeg", 'rb')
  # if response.code == 200
  #   # case JSON.parse(JSON.parse(response)["result"][0])["Result"]
  # end
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
  if code == "error_captcha_unsolvable"
    session.driver.quit  
    next 
  end
  input = session.find('#captchaInputContainer input')
  input.set(code.force_encoding('UTF-8').downcase)
  session.click_button('captchaSubmitButton') 
  sleep 5
  phone = parse_phone(session)
  phone.sub!(/\d\+/, ",+") if !phone.nil? && phone.length > 0
  puts phone
  # binding.pry
  if !phone.nil? && phone != "" 
    car = dataset.filter(id: car[:id])
    car.update(
      phone: phone
    )
    puts 'Updated'
    session.driver.quit  
    next
  end
  # session.save_screenshot('page.jpeg', :selector => '.adv-text')
  session.driver.quit  
end

