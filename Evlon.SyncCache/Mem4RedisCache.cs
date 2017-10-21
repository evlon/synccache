using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SyncCache;
using StackExchange.Redis;
using System.Collections;
using System.Collections.Generic;
using Newtonsoft.Json;
using NLog;

namespace SyncCache
{
    //public interface IMem4RedisCache
    //{
    //    string GetCacheKey(string key);
    //    T GetValue<T>(string key);
    //    bool TryGetValue<T>(string key, out T val);
    //    Task<T> GetValueAsync<T>(string key, Func<T> func);
    //    T GetValue<T>(string key, Func<T> func);
    //    Task RedisSetValueAsync<T>(string key, Func<T> func);
    //    void RedisSetValue<T>(string key, Func<T> func);
    //    Task RedisSetValueAsync<T>(string key, T val);
    //    Task RedisSetValueAsync<T>(string key, T val, double expire);
    //    void RedisSetValue<T>(string key, T val);
    //    void RedisSetValue<T>(string key, T val, TimeSpan expire);
    //    Task<bool> RedisSetAddValueAsync(string key, Func<string> func);
    //    void RedisSetAddValue(string key, Func<string[]> func);
    //    void RedisKeyRemove(string key);
    //    Task RedisKeyRemoveAsync(string key);
    //    bool GetSetValue(string key,string field);
    //}

    public interface IMem4RedisCache
    {
        string GetCacheKey(string key);
        T GetValue<T>(string key);
        bool TryGetValue<T>(string key, out T val);
        Task<T> GetValueAsync<T>(string key, Func<T> func,Func<T,bool> isValueCanCache = null );
        T GetValue<T>(string key, Func<T> func, Func<T,bool> isValueCanCache = null);
        Task RedisSetValueAsync<T>(string key, T val, TimeSpan? expire = null);
        Task RedisSetValueAsync<T>(string key, Func<T> func,TimeSpan? expire = null, Func<T,bool> isValueCanCache = null);
        void RedisSetValue<T>(string key, T val,TimeSpan? expire = null);
        void RedisSetValue<T>(string key, Func<T> func,TimeSpan? expire = null, Func<T,bool> isValueCanCache = null);
        //Task RedisSetValueAsync<T>(string key, T val);
        void RedisSetAddValue(string key, Func<string[]> func,TimeSpan?  expire = null);
        Task<bool> RedisSetAddValueAsync(string key, Func<string> func, TimeSpan? expire = null);
        void RedisKeyRemove(string key);
        Task RedisKeyRemoveAsync(string key);
        bool GetSetValue(string key,string field);
    }

    public class Mem4RedisCache : IMem4RedisCache
    {
        private static ILogger _logger = LogManager.GetCurrentClassLogger();
        ///静态单一示例，节省连接资源
        private static Lazy<RedisKeyspaceSubscriber> _keyspaceSubscriberLazy;

        private static string _memcacheRedisKeyPrefix = "memcache#";

        private static MemoryObjectCache _cache = new MemoryObjectCache();
        //private readonly EnumRedisDb _db;
        private readonly string _keyPrefix;
        private readonly int _expireSeconds;
        private readonly RedisClient _redisClient;

        private RedisClient GetRedisClient()
        {
            return _redisClient;
        }

   

        public Mem4RedisCache(RedisClient redisClient,EnumRedisDb db,string keyPrefix, int expireSeconds = 20* 60)
        {
            //_db = 0;// db;
            _keyPrefix = keyPrefix;
            _expireSeconds = expireSeconds;
            if (_keyspaceSubscriberLazy == null)
            {
                _keyspaceSubscriberLazy =
                    new Lazy<RedisKeyspaceSubscriber>(
                        () =>
                            new RedisKeyspaceSubscriber(redisClient.GetRedis(), KeyChangeOrExpired,
                                _memcacheRedisKeyPrefix));

                Task.Factory.StartNew(() => {
                    var val = _keyspaceSubscriberLazy.Value;
                    _logger.Debug($"订阅成功：{val.Redis.GetStatus()}");
                });
            }

            _redisClient  = redisClient;

        }

        private static void KeyChangeOrExpired(string key)
        {
            _cache.Remove(key);
        }

        public Mem4RedisCache(EnumRedisDb db, string keyPrefix, int expireSeconds = 20 * 60):
            this(RedisClient.GetClient(db),db,keyPrefix,expireSeconds )
        {
          
        }
        public Mem4RedisCache(string keyPrefix, int expireSeconds = 20 * 60) :
                   this(EnumRedisDb.Default, keyPrefix, expireSeconds)
        {

        }
        //public void InitSubscriber()
        //{
        //    _keyspaceSubscriberLazy.Value.InitSubscrib();

        //    var redis = _redisClient.GetRedis();
        //    string keyspace = string.Concat("__keyspace@", (int)_db, "__:");

        //    redis.ClusterSubscribe(string.Concat("__keyspace@", (int)_db, "__:", _keyPrefix, "*"), (rc, rv) =>
        //    {
        //        //__keyspace@0__:k, set
        //        string key = rc.ToString().Substring(keyspace.Length);
        //        GetMemCache().Remove(key);

        //    });

        //    //redis.ClusterSubscribe("*", (rc, rv) =>
        //    //{
        //    //    //__keyspace@0__:k, set
        //    //    Console.WriteLine($"RC:{rc}   rv:{rv}");

        //    //}); ;
        //    //;
        //    //Task.Factory.StartNew(async () =>
        //    //{
        //    //    while (true)
        //    //    {

        //    //        var endPoints = redis.GetEndPoints();
        //    //        foreach (var endPoint in endPoints)
        //    //        {
        //    //            try
        //    //            {
        //    //                Console.WriteLine($"{endPoint} {redis.GetServer(endPoint).SubscriptionPatternCount()}");
        //    //            }
        //    //            catch (Exception ex)
        //    //            {
        //    //                Console.WriteLine($"{endPoint} ERR");
        //    //            }
        //    //        }

        //    //        await Task.Delay(1000);
        //    //    }
        //    //});

        //}

        public static string MakeRedisKey(string key)
        {
            //__keyspace@0__:memcache#dangjian_whitelist  sadd
            return string.Concat(_memcacheRedisKeyPrefix, key);
        }

        public string GetCacheKey(string key)
        {
            return string.Concat(_keyPrefix, key);
        }

        public T GetValue<T>(string key)
        {
            T val;
            if (TryGetValue(key, out val))
                return val;

            return default(T);
        }

        public bool TryGetValue<T>(string key, out T val)
        {
#if DEBUG
            if (!key.StartsWith(_keyPrefix))
            {
                throw new ArgumentOutOfRangeException("key", "key必须使用前缀：" + _keyPrefix);
            }
#endif

            if (_cache.TryGet(key, out val))
            {
                // 到，直接返回
                return true;
            }
            else
            {
                string redisKey = MakeRedisKey(key);
                //从Redis拿，如果没有从func 里拿
                if (GetRedisClient().TryGet<T>(redisKey, out val))
                {
                    _cache.AddOrUpdate(key, val, TimeSpan.FromSeconds(_expireSeconds));
                    return true;
                }
                else
                {
                    return false;
                }

            }
        }

     

        public async Task<T> GetValueAsync<T>(string key, Func<T> func, Func<T,bool> isValueCanCache = null)
        {
#if DEBUG
            if (!key.StartsWith(_keyPrefix))
            {
                throw new ArgumentOutOfRangeException("key","key必须使用前缀：" + _keyPrefix);
            }
#endif

            T val;
            if (_cache.TryGet(key, out val))
            {
                //找到，直接返回
                return val;
            }
            else
            {
                string redisKey = MakeRedisKey(key);
                //从Redis拿，如果没有从func 里拿
                val = await GetRedisClient().GetAsync<T>(redisKey, func, TimeSpan.FromSeconds(_expireSeconds),isValueCanCache);

                _cache.AddOrUpdate(key,val,TimeSpan.FromSeconds(_expireSeconds));
                return val;
            }
        }


        public T GetValue<T>(string key, Func<T> func, Func<T,bool> isValueCanCache = null)
        { 
#if DEBUG
            if (!key.StartsWith(_keyPrefix))
            {
                throw new ArgumentOutOfRangeException("key", "key必须使用前缀：" + _keyPrefix);
            }
#endif

            T val;
            if (_cache.TryGet(key, out val))
            {
                //找到，直接返回
                return val;
            }
            else
            {
                string redisKey = MakeRedisKey(key);
                //从Redis拿，如果没有从func 里拿
                val = GetRedisClient().Get<T>(redisKey, func, TimeSpan.FromSeconds(_expireSeconds),isValueCanCache);

                _cache.AddOrUpdate(key, val, TimeSpan.FromSeconds(_expireSeconds));
                return val;
            }
        }
        public async Task RedisSetValueAsync<T>(string key, Func<T> func,TimeSpan? expire = null, Func<T, bool> isValueCanCache = null)
        {
            string redisKey = MakeRedisKey(key);
            var val = await Task.Run(func);
            if (isValueCanCache != null && isValueCanCache(val))
            {
                expire = expire ?? TimeSpan.FromSeconds(_expireSeconds);

                await GetRedisClient().SetAsync(redisKey, val, expire);
            }
        }
        public void RedisSetValue<T>(string key, Func<T> func,TimeSpan? expire = null, Func<T,bool> isValueCanCache = null)
        {
            string redisKey = MakeRedisKey(key);
            var val = func();
            if (isValueCanCache != null && isValueCanCache(val))
            {
                GetRedisClient().Set(redisKey, val, expire ?? TimeSpan.FromSeconds(_expireSeconds));
            }
        }

        public async Task RedisSetValueAsync<T>(string key, T val,TimeSpan? expire = null)
        {
            string redisKey = MakeRedisKey(key);
            var db = GetRedisClient();
            var ret =await db.SetAsync(redisKey, val, expire??TimeSpan.FromSeconds(_expireSeconds));
        }
        //public async Task RedisSetValueAsync<T>(string key, T val, double expire)
        //{
        //    string redisKey = MakeRedisKey(key);
        //    await GetRedisClient().SetAsync(redisKey, val, expire);
        //}
        public void RedisSetValue<T>(string key, T val,TimeSpan? expire)
        {
            string redisKey = MakeRedisKey(key);

            GetRedisClient().Set(redisKey, val,expire??TimeSpan.FromSeconds(_expireSeconds));
        }

        //public void RedisSetValue<T>(string key, T val, TimeSpan? expire= null)
        //{
        //    string redisKey = MakeRedisKey(key);

        //    GetRedisClient().Set(redisKey, val, expire ?? TimeSpan.FromSeconds(_expireSeconds));
        //}

        public async Task<bool> RedisSetAddValueAsync(string key, Func<string> func, TimeSpan? expire = null)
        {
            string redisKey = MakeRedisKey(key);

            var t = await Task.Run(func);
            return   await GetRedisClient().SetAddValueAsync(redisKey, t,expire??TimeSpan.FromSeconds(_expireSeconds));
             
            
        }
        public void RedisSetAddValue(string key, Func<string[]> func,TimeSpan? expire = null)
        {
            string redisKey = MakeRedisKey(key);
            var val = func();
            GetRedisClient().SetAddValue(redisKey, val,expire??TimeSpan.FromSeconds(_expireSeconds));
        }

        public void RedisKeyRemove(string key)
        {
            string redisKey = MakeRedisKey(key);
            GetRedisClient().KeyDelete(redisKey);
        }

        public async Task RedisKeyRemoveAsync(string key)
        {
            string redisKey = MakeRedisKey(key);
            await GetRedisClient().KeyDeleteAsync(redisKey);
        }

        public bool GetSetValue(string key,string field)
        {
#if DEBUG
            if (!key.StartsWith(_keyPrefix))
            {
                throw new ArgumentOutOfRangeException("key", "key必须使用前缀：" + _keyPrefix);
            }
#endif

            HashSet<string> val;
            //去Mem取
            if (!_cache.TryGet(key, out val))
            {
                string redisKey = MakeRedisKey(key);
                //从Redis拿
                RedisValue[] rvVal;
                rvVal = GetRedisClient().GetSetValue(redisKey);

                val = new HashSet<string>();
                foreach (var item in rvVal)
                {
                    if (item.HasValue)
                    {
                        var value = JsonConvert.DeserializeObject<string>(item);
                        val.Add(value);
                    }

                }

                _cache.AddOrUpdate(key, val, TimeSpan.FromSeconds(_expireSeconds));  
            }
           
            return val.Contains(field);
        }

    }

    //public static class CacheKey
    //{
    //    public static readonly string UserInfo = "user.info";
    //    public static readonly string UserAuth = "user.auth.";
    //    public static readonly string Web = "web.";
    //    public static readonly string Sys = "sys.";
    //    public static readonly string SysTongji = "sys.tongji";

    //}


}
