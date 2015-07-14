require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe Mongo::Queue do
  
  before(:all) do
    opts   = {
      :database   => 'mongo_queue_spec',
      :collection => 'spec',
      :attempts   => 4,
      :timeout    => 60}
    Database = Moped::Session.new(['localhost:27017'])
    Queue    = Mongo::Queue.new(Database, opts)
  end
  
  before(:each) do
    Queue.flush!
  end
  
  describe "Configuration" do

    it "should set the connection" do
      Queue.connection.should be(Database)
    end

    it "should allow database option" do
      Queue.config[:database].should eql('mongo_queue_spec')
    end
    
    it "should allow collection option" do
      Queue.config[:collection].should eql('spec')
    end

    it "should allow attempts option" do
      Queue.config[:attempts].should eql(4)
    end
  
    it "should allow timeout option" do
      Queue.config[:timeout].should eql(60)
    end
  
    it "should have a sane set of defaults" do
      q = Mongo::Queue.new(Moped::Session.new(['localhost:27017']))
      q.config[:collection].should eql 'mongo_queue'
      q.config[:attempts].should   eql 3
      q.config[:timeout].should    eql 300
    end
  end

  describe "Inserting a Job" do
    before(:each) do
      Queue.insert(:message => 'MongoQueueSpec')
      @item = Queue.send(:collection).find.first
    end

    it "should set priority to 0 by default" do
      @item['priority'].should be(0)
    end

    it "should set a null locked_by" do
      @item['locked_by'].should be(nil)      
    end

    it "should set a null locked_at" do
      @item['locked_at'].should be(nil)
    end

    it "should allow additional fields" do
      @item['message'].should eql('MongoQueueSpec')
    end

    it "should set a blank last_error" do
      @item['last_error'].should be(nil)
    end

    it "should set the time of insertion" do
      #5 milliseconds is MORE than enough
      @item['created_at'].should >= Time.now.utc - 5
      @item['created_at'].should <= Time.now.utc + 5
    end
  end
    
  describe "Queue Information" do
    it "should provide a convenience method to retrieve stats about the queue" do
      Queue.stats.should eql({
        :locked    => 0,
        :available => 0,
        :errors    => 0,
        :total     => 0
      })
    end
    
    it "should calculate properly" do
      @first  = Queue.insert(:msg => 'First',  :attempts => 4)
      @second = Queue.insert(:msg => 'Second', :priority => 2)
      @third  = Queue.insert(:msg => 'Third',  :priority => 6)
      @fourth = Queue.insert(:msg => 'Fourth', :locked_by => 'Example', :locked_at => Time.now.utc - 60 * 60 * 60, :priority => 99)
      Queue.stats.should eql({
        :locked    => 1,
        :available => 2,
        :errors    => 1,
        :total     => 4
      })
    end
  end
  
  describe "Working with the queue" do
    before(:each) do
      @first  = Queue.insert(:msg => 'First')
      @second = Queue.insert(:msg => 'Second', :priority => 2)
      @third  = Queue.insert(:msg => 'Third',  :priority => 6)
      @fourth = Queue.insert(:msg => 'Fourth', :locked_by => 'Example', :locked_at => Time.now.utc - 60 * 60 * 60, :priority => 99)
    end

    it "should find jobs" do
      query = Queue.find(:msg => 'First')
      query.first['msg'].should eql('First')
      query.count.should eql(1)
    end
    
    it "should lock the next document by priority" do
      doc = Queue.lock_next('Test')
      doc['msg'].should eql('Third')
    end
    
    it "should release and relock the next document" do
      Queue.release(@fourth, 'Example')
      Queue.lock_next('Bob')['msg'].should eql('Fourth')
    end
    
    it "should remove completed items" do
      doc = Queue.lock_next('grr')
      Queue.complete(doc,'grr')
      Queue.lock_next('grr')['msg'].should eql('Second')
    end
    
    it "should return nil when unable to lock" do
      4.times{ Queue.lock_next('blah') }
      Queue.lock_next('blah').should eql(nil)
    end

    it "should remove requested documents" do
      Queue.remove(:priority => {'$gt' => 1 })
      Queue.lock_next('woo')
      Queue.lock_next('waa').should eql(nil)
    end

    it "should modify the requested document" do
      Queue.modify({ msg: 'Second' }, { priority: 999 })
      Queue.lock_next('woo')['msg'].should eql('Second')
    end

    it "should modify the requested document via upsert" do
      Queue.modify({ msg: 'First' }, { priority: 999 })
      Queue.lock_next('woo')['msg'].should eql('First')
    end

    it "should return nil on modify failure" do
      Queue.modify({ msg: 'Not Found' }, {}).should eql(nil)
    end
  end

  describe "Queue documents with time restrictions" do
    it "should not lock documents that are not active yet" do
      doc = Queue.insert(:active_at => Time.now.utc + 100)
      Queue.lock_next('patooey').should eql(nil)
    end

    it "should lock documents that are active" do
      doc = Queue.insert(:active_at => Time.now.utc - 100, :msg => "Success")
      Queue.lock_next('gotcha')['msg'].should eql("Success")
    end
  end
  
  describe "Error Handling" do
    it "should allow document error handling" do
      doc = Queue.insert(:stuff => 'Broken')

      #first attempt
      Queue.error(doc, 'I think I broke it')

      #second attempt
      doc['active_at'] = retry_at = Time.now.utc - 100
      Queue.error(doc, 'Yup, broke it')

      doc = Queue.lock_next('Money')
      doc['attempts'].should eql(2)
      doc['active_at'].to_i.should eql(retry_at.to_i)
      doc['last_error'].should eql('Yup, broke it')
    end
  end
  
  describe "Cleaning up" do
    it "should remove all of the stale locks" do
      Queue.insert(:msg => 'Fourth', :locked_by => 'Example',
                   :locked_at => Time.now.utc - 60 * 60 * 60, :priority => 99)
      Queue.cleanup!
      Queue.lock_next('Foo')['msg'].should eql('Fourth')
    end
  end
    
end
