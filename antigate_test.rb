require 'nokogiri'
require 'capybara'
require 'capybara/dsl'
require 'capybara/poltergeist'
require 'pry'
require 'ap'
require "sequel"

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

def click_show_buttons(session)
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
end

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

DB = Sequel.connect('postgres://localhost/car_monitor_development')
dataset = DB[:cars]
dataset.where(sold: false, source_removed:false, closed: false, phone: "").
          or(sold: false, source_removed:false, closed: false, phone: nil).
          reverse_order(:created_at).each do |car|
# dataset.where('sold = false AND source_removed = false AND closed = false').where(phone: "").order(:created_at).each do |car|
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
  phone = ""
  phone = parse_phone(session)
  phone.sub!(/\d\+/, ",+") if !phone.nil? && phone.length > 0
  puts phone
  if !phone.nil? && phone != "" 
    car = dataset.filter(id: car[:id])
    car.update(
      phone: phone
    )
    puts 'Updated'
    sleep rand(10..20)
  else
    doc = Nokogiri::HTML(session.html)
    closed = doc.css('span.warning strong').text.include?("Объявление находится в архиве")
    sold = doc.css('span.warning strong').text.include?("продан")
    source_removed = doc.css('.adv-text .b-media-cont.b-media-cont_relative').first ? false : true
    if sold || closed || source_removed
      car = dataset.filter(id: car[:id])
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
        phone = parse_phone(session)
        phone.sub!(/\d\+/, ",+") if !phone.nil? && phone.length > 0
        puts phone
        if !phone.nil? && phone != "" 
          car = dataset.filter(id: car[:id])
          car.update(
            phone: phone
          )
          puts 'Updated'
        end
        # session.save_screenshot('page.jpeg', :selector => '.adv-text')
      end
    end
  end
  session.driver.quit  
end
