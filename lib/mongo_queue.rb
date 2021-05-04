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
    :priority      => 0,
    :attempts      => 0,
    :locked_by     => nil,
    :locked_at     => nil,
    :keep_alive_at => nil,
    :last_error    => nil,
    :active_at     => nil
    #created_at    => Time.now.utc
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
    @config = DEFAULT_CONFIG.merge(opts)
    @connection = connection.use(@config[:database])
  end

  # Remove all jobs that have been retried past the maximum attempts and place
  # them in a backup collection, to avoid growing the collection size indefinitely.
  def purge!
    cursor = collection.find({
      :locked_by => nil,
      :attempts  => { '$gte' => @config[:attempts] }
    })

    # continue_on_error is not an option for insert_many , use unordered inserts instead.By specifying ordered: false ,
    # inserts will happen in an unordered fashion and it will try to insert all requests.
    # Including an try catch block will make sure it won't break after an exception,
    # so you are achieving an MYSQL INSERT IGNORE equivalent.
    begin
      purged_collection.insert_many(cursor.no_cursor_timeout.to_a, {ordered: false})
    rescue => e
      puts "ERROR #{e.message.inspect}"
    end
    # this will reduce soter_queue delete database call
    cursor.delete_many
  end

  # Insert a new item in to the queue with required queue message parameters.
  #
  # Example:
  #    queue.insert(:name => 'Billy', :email => 'billy@example.com', :message => 'Here is the thing you asked for')
  def insert(hash)
    document = DEFAULT_INSERT.merge(:_id => BSON::ObjectId.new,
                                    :created_at => Time.now.utc).merge(hash)
    collection.insert_one(document)
    collection.find(:_id => document[:_id]).first
  end

  def find(query)
    collection.find(query)
  end

  # Modify an existing item in the queue by upserting the requested changes.
  # Only changes the first document found.
  def modify(query, changes)
    cmd = {}
    cmd['findandmodify'] = @config[:collection]
    cmd['update']        = {
      '$set' => changes
    }
    cmd['query']         = query
    cmd['sort']          = sort_hash
    cmd['limit']         = 1
    cmd['new']           = true
    run(cmd)
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
        :locked_by     => locked_by,
        :locked_at     => Time.now.utc,
        :keep_alive_at => Time.now.utc
      }
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
    collection.delete_many(hash)
  end

  # Removes stale locks that have exceeded the timeout and places them back in the queue.
  def cleanup!
    cursor =
      collection.find(:locked_by     => {'$ne' => nil},
                      :keep_alive_at => {'$lt' => Time.now.utc - config[:timeout]})

    cursor.no_cursor_timeout.each do |doc|
      release(doc, doc['locked_by'])
    end
  end

  # Release a lock on the specified document and allow it to become available again.
  def release(doc, locked_by)
    cmd = {}
    cmd['findandmodify'] = @config[:collection]
    cmd['update']        = {'$set' => {
                              :locked_by     => nil,
                              :locked_at     => nil,
                              :keep_alive_at => nil
                            }}
    cmd['query']         = {:locked_by => locked_by,
      :_id => BSON::ObjectId.from_string(doc['_id'].to_s)}
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
      :_id => BSON::ObjectId.from_string(doc['_id'].to_s)}
    cmd['remove']        = true
    cmd['limit']         = 1
    run(cmd)
  end

  # Increase the error count on the locked document and release. Optionally provide an error message.
  def error(doc, error_message=nil)
    collection.update_one({ :_id => doc['_id'] },
                          {
                            '$set' => {
                              'last_error'    => error_message,
                              'locked_by'     => nil,
                              'locked_at'     => nil,
                              'keep_alive_at' => nil,
                              'active_at'     => doc['active_at']
                            },
                            '$inc' => {
                              'attempts'   => 1
                            }
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
                var a  = db.#{config[:collection]}.count({'locked_by': null, 'attempts': {$lt: #{config[:attempts]}}});
                var ac = db.#{config[:collection]}.count({'locked_by': null, 'attempts': {$lt: #{config[:attempts]}}, '$or': [{'active_at': null}, {'active_at': {'$lt': new Date()}}]});
                var l  = db.#{config[:collection]}.count({'locked_by': /.*/});
                var e  = db.#{config[:collection]}.count({'attempts': {$gte: #{config[:attempts]}}});
                var t  = db.#{config[:collection]}.count();
                return [a, ac, l, e, t];
              }
            );
          }"

    available, active, locked, errors, total =
     collection.database.command(:'$eval' => js).documents.first['retval']

    { :available => available.to_i,
      :active    => active.to_i,
      :locked    => locked.to_i,
      :errors    => errors.to_i,
      :total     => total.to_i }
  end

  protected

  def sort_hash #:nodoc:
    sh = {}
    sh['priority'] = -1 ; sh
  end

  def value_of(result) #:nodoc:
    result.documents.first['ok'] == 0 ? nil : result.documents.first['value']
  end

  def run(cmd) #:nodoc:
    value_of collection.database.command(cmd)
  end

  def collection #:nodoc:
    @connection[(@config[:collection])]
  end

  def purged_collection #:nodoc:
    @connection[("#{@config[:collection]}_purged")]
  end
end
