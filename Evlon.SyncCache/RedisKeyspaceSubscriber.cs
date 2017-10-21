using System;
using System.Runtime.Remoting.Contexts;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using StackExchange.Redis;

namespace SyncCache
{
    public class RedisKeyspaceSubscriber
    {
        private static ILogger _logger = LogManager.GetCurrentClassLogger();
        private readonly Action<string> _onKeyExpire;
        private readonly bool _isRedisCluster;
        private readonly int _db;
        private bool _isSubscribed;
        private object initLocker = new object();
        public IConnectionMultiplexer Redis { get; }
        public string KeyPrefix { get; }
        
        public RedisKeyspaceSubscriber(IConnectionMultiplexer redis, Action<string> onKeyExpire,string keyPrefix, bool isRedisCluster=true, int db = 0)
        {
            if (redis == null) throw new ArgumentNullException("redis");
            if (onKeyExpire == null) throw new ArgumentNullException("onKeyExpire");
            if (keyPrefix == null) throw new ArgumentNullException("keyPrefix");
            _onKeyExpire = onKeyExpire;
            _isRedisCluster = isRedisCluster;
            _db = db;
            Redis = redis;
            KeyPrefix = keyPrefix;

            InitSubscrib();
        }

        protected void InitSubscrib()
        {
            if (_isSubscribed)
                return;


            Task.Factory.StartNew(() =>
            {

                if (_isSubscribed) return;
                lock (initLocker)
                {
                    if (_isSubscribed) return;

                    try
                    {
                        DoInitSubscrib();
                        _isSubscribed = true;
                    }
                    catch (Exception ex)
                    {
                        _logger.Error(ex, "初始化订阅失败：" + ex.Message);
                    }
                }
            });


        }

        private  void DoInitSubscrib()
        {
            var redis = Redis;
            var keyPrefix = KeyPrefix;
            //__keyspace@0__:memcache#
            string keyspace = string.Concat("__keyspace@", (int)_db, "__:", keyPrefix);
            string channel = string.Concat(keyspace, "*");

            if (_isRedisCluster)
            {

                redis.ClusterSubscribe(channel, (rc, rv) =>
                {
                    //__keyspace@0__:k, set
                    string key = rc.ToString().Substring(keyspace.Length);
                    _onKeyExpire(key);

                });
            }
            else
            {
                redis.GetSubscriber(this).Subscribe(channel, (rc, rv) =>
                {
                    //__keyspace@0__:k, set
                    string key = rc.ToString().Substring(keyspace.Length);
                    _onKeyExpire(key);
                });

            }
        }
    }

}