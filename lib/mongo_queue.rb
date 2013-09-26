module Mongo
end

class Mongo::Queue
  attr_reader :connection, :config

  DEFAULT_CONFIG = {
    :database   => 'mongo_queue',
    :collection => 'mongo_queue',
    :timeout    => 300,
    :attempts   => 3
  }.freeze

  DEFAULT_INSERT = {
    :priority   => 0,
    :attempts   => 0,
    :locked_by  => nil,
    :locked_at  => nil,
    :last_error => nil
    #created_at => Time.now.utc
  }.freeze

  # Create a new instance of MongoQueue with the provided mongodb connection and optional configuration.
  # See +DEFAULT_CONFIG+ for default configuration and possible configuration options.
  #
  # Example:
  #    db = Mongo::Connection.new('localhost')
  #    config = {:timeout => 90, :attempts => 2}
  #    queue = Mongo::Queue.new(db, config)
  #
  def initialize(connection, opts={})
    @connection = connection
    @config = DEFAULT_CONFIG.merge(opts)
    @connection.use(@config[:database])
  end

  # Remove all items from the queue. Use with caution!
  def flush!
    collection.drop
  end

  # Insert a new item in to the queue with required queue message parameters.
  #
  # Example:
  #    queue.insert(:name => 'Billy', :email => 'billy@example.com', :message => 'Here is the thing you asked for')
  def insert(hash)
    document = DEFAULT_INSERT.merge(:_id => Moped::BSON::ObjectId.new,
                                    :created_at => Time.now.utc).merge(hash)
    collection.insert(document)
    collection.find(:_id => document[:_id]).first
  end

  # Lock and return the next queue message if one is available. Returns nil if none are available. Be sure to
  # review the README.rdoc regarding proper usage of the locking process identifier (locked_by).
  # Example:
  #    locked_doc = queue.lock_next(Thread.current.object_id)
  def lock_next(locked_by)
    cmd = {}
    cmd['findandmodify'] = @config[:collection]
    cmd['update']        = {
      '$set' => {
        :locked_by => locked_by,
        :locked_at => Time.now.utc }
    }
    cmd['query']         = {
      :locked_by => nil,
      :attempts  => { '$lt' => @config[:attempts] },
      '$or' => [{:active_at => nil},
                {:active_at => {'$lt' => Time.now.utc}}]
    }
    cmd['sort']          = sort_hash
    cmd['limit']         = 1
    cmd['new']           = true
    run(cmd)
  end

  def remove(hash)
    collection.find(hash).remove_all
  end

  # Removes stale locks that have exceeded the timeout and places them back in the queue.
  def cleanup!
    cursor =
      collection.find(:locked_by => /.*/,
                      :locked_at => {'$lt' => Time.now.utc - config[:timeout]})

    cursor.each do |doc|
      release(doc, doc['locked_by'])
    end
  end

  # Release a lock on the specified document and allow it to become available again.
  def release(doc, locked_by)
    cmd = {}
    cmd['findandmodify'] = @config[:collection]
    cmd['update']        = {'$set' => {:locked_by => nil, :locked_at => nil}}
    cmd['query']         = {:locked_by => locked_by,
      :_id => Moped::BSON::ObjectId.from_string(doc['_id'].to_s)}
    cmd['limit']         = 1
    cmd['new']           = true
    run(cmd)
  end

  # Remove the document from the queue. This should be called when the work is done and the document is no longer needed.
  # You must provide the process identifier that the document was locked with to complete it.
  def complete(doc, locked_by)
    cmd = {}
    cmd['findandmodify'] = @config[:collection]
    cmd['query']         = {:locked_by => locked_by, 
      :_id => Moped::BSON::ObjectId.from_string(doc['_id'].to_s)}
    cmd['remove']        = true
    cmd['limit']         = 1
    run(cmd)
  end

  # Increase the error count on the locked document and release. Optionally provide an error message.
  def error(doc, error_message=nil)
    collection.find(:_id => doc['_id']).
      update(
             '$set' => {
               'last_error' => error_message,
               'locked_by'  => nil,
               'locked_at'  => nil,
               'active_at'  => doc['active_at']
             },
             '$inc' => {
               'attempts'   => 1
             })
  end

  # Provides some information about what is in the queue. We are using an eval to ensure that a
  # lock is obtained during the execution of this query so that the results are not skewed.
  # please be aware that it will lock the database during the execution, so avoid using it too
  # often, even though it it very tiny and should be relatively fast.
  def stats
    js = "function queue_stat(){
              return db.eval(
              function(){
                var a = db.#{config[:collection]}.count({'locked_by': null, 'attempts': {$lt: #{config[:attempts]}}});
                var l = db.#{config[:collection]}.count({'locked_by': /.*/});
                var e = db.#{config[:collection]}.count({'attempts': {$gte: #{config[:attempts]}}});
                var t = db.#{config[:collection]}.count();
                return [a, l, e, t];
              }
            );
          }"

    available, locked, errors, total =
      collection.database.command(:'$eval' => js)['retval']

    { :locked    => locked.to_i,
      :errors    => errors.to_i,
      :available => available.to_i,
      :total     => total.to_i }
  end


  protected

  def sort_hash #:nodoc:
    sh = {}
    sh['priority'] = -1 ; sh
  end

  def value_of(result) #:nodoc:
    result['okay'] == 0 ? nil : result['value']
  end

  def run(cmd) #:nodoc:
    begin
      value_of collection.database.command(cmd)
    rescue Mongo::OperationFailure
      nil
    end
  end

  def collection #:nodoc:
    @connection[(@config[:collection])]
  end
end
