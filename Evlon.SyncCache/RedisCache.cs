using System;
using System.Collections.Concurrent;
using System.Configuration;
using System.Threading.Tasks;
using Newtonsoft.Json;
using NLog;
using SyncCache;

namespace SyncCache
{
    public class RedisCache
    {
        private static ILogger _logger = LogManager.GetCurrentClassLogger();
        private static ConcurrentDictionary<EnumRedisDb, CacheKeyRemover> _keyRmovers = new ConcurrentDictionary<EnumRedisDb, CacheKeyRemover>();
        private static ConcurrentDictionary<EnumRedisDb, double> _keyExpire = new ConcurrentDictionary<EnumRedisDb, double>();
        private static ConcurrentDictionary<EnumRedisDb, CacheKeySetter> _keySetAdders = new ConcurrentDictionary<EnumRedisDb, CacheKeySetter>();

        public static double GetCacheKeyExpire(EnumRedisDb db)
        {
            return _keyExpire.GetOrAdd(db, d =>
            {
                var keyExpire = ConfigurationManager.AppSettings[string.Concat("RedisCache.Expire.", (int) db)] ??
                                ConfigurationManager.AppSettings["RedisCache.Expire"] ?? "3600";

                double expireSecond = int.Parse(keyExpire);
                return expireSecond;
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="expire">seconds</param>
        public static void SetCacheKeyExpire(EnumRedisDb db, double expire)
        {
            _keyExpire[db] = expire;
        }

        public static TRet GetOrSetAsync<T,TRet>(EnumRedisDb db,string cacheKey, Func<T, TRet> func, T arg)
        {
            using (var rc = RedisClient.GetClient(db))
            {
                return rc.GetOrSetAsync(cacheKey, GetCacheKeyExpire(db), func, arg);
              
            }
        }


        //public static TRet GetOrSetAsync<T, TRet>(EnumRedisDb db, string cacheKey, bool isSelf, string type, Func<T, TRet> func, T arg)
        //{
        //    using (var rc = RedisClient.GetClient(db))
        //    {
        //        var ret = rc.Get<TRet>(cacheKey);
        //        if (object.ReferenceEquals(ret, default(TRet)))
        //        {
        //            // not found
        //            var val = func(arg);
        //            Task.Factory.StartNew(async () =>
        //            {
        //                using (var rcInsert = RedisClient.GetClient(db))
        //                {
        //                    await rcInsert.SetAsync(cacheKey, val,GetCacheKeyExpire(db));
        //                }

        //            });
        //            if (isSelf && type=="党支部")
        //            {
                        
                        
        //            }
        //            return val;
        //        }
        //        else
        //        {
        //            return ret;
        //        }

        //    }
        //}

        public static TRet GetOrSetAsync<T1,T2, TRet>(EnumRedisDb db, string cacheKey, Func<T1,T2, TRet> func, T1 arg1,T2 arg2)
        {
            using (var rc = RedisClient.GetClient(db))
            {
                return rc.GetOrSetAsync(cacheKey, GetCacheKeyExpire(db), func, arg1,arg2);
                //var ret = rc.Get<TRet>(cacheKey);
                //if (object.ReferenceEquals(ret, default(TRet)))
                //{
                //    // not found
                //    var val = func(arg1,arg2);
                //    Task.Factory.StartNew(async () =>
                //    {
                //        using (var rcInsert = RedisClient.GetClient(db))
                //        {
                //            await rcInsert.SetAsync(cacheKey, val, GetCacheKeyExpire(db));
                //        }

                //    });

                //    return val;
                //}
                //else
                //{
                //    return ret;
                //}

            }
        }

        public static void CommitSetCache<T, TRet>(EnumRedisDb db, string cacheKey, Func<T, TRet> func, T arg)
        {
            Task.Factory.StartNew(async () =>
            {
                try
                {
                    await SetAsync(db, cacheKey, func, arg);
                }
                catch (Exception ex)
                {
                    _logger.Error(ex, $"CommitSetCache Error{ex.Message}");
                }
            });

        }

        public static void CommitSetCache<T>(EnumRedisDb db, string cacheKey,  T val,TimeSpan? expire = null)
        {
            
            Task.Factory.StartNew(async () =>
            {
                try
                {
                    await SetAsync(db, cacheKey, val,expire);
                }
                catch (Exception ex)
                {
                    _logger.Error(ex, $"CommitSetCache Error{ex.Message}");
                }
            });
        }


        public static async Task SetAsync<T, TRet>(EnumRedisDb db, string cacheKey, Func<T, TRet> func, T arg, TimeSpan? expire = null)
        {
            // not found
            //var val = await Task.Run(()=>func(arg));

            using (var rcInsert = RedisClient.GetClient(db))
            {
                await rcInsert.SetAsync(cacheKey, expire?.TotalSeconds ?? GetCacheKeyExpire(db), func, arg);
            }

        }
        public static async Task SetAsync<T>(EnumRedisDb db, string cacheKey, T val, TimeSpan? expire = null)
        {
            // not found
            using (var rcInsert = RedisClient.GetClient(db))
            {
                await rcInsert.SetAsync(cacheKey, val, expire?.TotalSeconds ?? GetCacheKeyExpire(db));
            }

        }

        public static bool CommitRemoveKey(EnumRedisDb db, string key)
        {
            return _keyRmovers.GetOrAdd(db,k=>new CacheKeyRemover(k)).CommitRemoveKey(key);
        }

        public static bool CommitStringAddKey(EnumRedisDb db, string key, string val, double? expire)
        {
            return _keySetAdders.GetOrAdd(db, k => new CacheKeySetter(k)).CommitSetAddKey(key,val,expire);
        }

       


        public static bool CommitStringAddKey<T>(EnumRedisDb db, string key, T val, TimeSpan? expire)
        {
            string strVal = JsonConvert.SerializeObject(val);
            double? expireVal = null;
            if (expire.HasValue)
            {
                expireVal = expire.Value.TotalSeconds;
            }
            return CommitStringAddKey(db,key,strVal,expireVal);
        }
    }

    public static class RedisClientCacheExtension
    {
        private static ILogger _logger = LogManager.GetCurrentClassLogger();



        public static TRet GetOrSetAsync<T, TRet>(this RedisClient rc, string cacheKey, double expire, Func<T, TRet> func, T arg)
        {
            TRet ret;
            if (rc.TryGet<TRet>(cacheKey, out ret))
                return ret;

            // not found
            var val = func(arg);
            Task.Factory.StartNew(async () =>
            {
                await rc.SetAsync(cacheKey, val, expire);
            });

            return val;
               
            
        }


        public static async Task<TRet> GetAsyncOrSetAsync<T, TRet>(this RedisClient rc, string cacheKey, double expire,
            Func<T, TRet> func, T arg)
        {
            var ret = await rc.GetAsync<TRet>(cacheKey);
            if (!object.ReferenceEquals(ret, default(TRet)))
                return ret;

            // not found
            var val = func(arg);
            await rc.SetAsync(cacheKey, val, expire);
            return val;


        }


        public static TRet GetOrSetAsync<T1,T2, TRet>(this RedisClient rc, string cacheKey, double expire, Func<T1,T2, TRet> func, T1 arg1,T2 arg2)
        {
            TRet ret;
            if (rc.TryGet<TRet>(cacheKey, out ret))
                return ret;

            // not found
            var val = func(arg1,arg2);
            Task.Factory.StartNew(async () =>
            {
                await rc.SetAsync(cacheKey, val, expire);
            });

            return val;


        }


        public static async Task<TRet> GetAsyncOrSetAsync<T1,T2, TRet>(this RedisClient rc, string cacheKey, double expire,
            Func<T1,T2, TRet> func, T1 arg1,T2 arg2)
        {
            var ret = await rc.GetAsync<TRet>(cacheKey);
            if (!object.ReferenceEquals(ret, default(TRet)))
                return ret;

            // not found
            var val = func(arg1,arg2);
            await rc.SetAsync(cacheKey, val, expire);
            return val;


        }

        public static void CommitSetCache<T, TRet>(this RedisClient rc, string cacheKey, double expire, Func<T, TRet> func, T arg)
        {
            Task.Factory.StartNew(async () =>
            {
                try
                {
                    await SetAsync(rc, cacheKey, expire, func, arg);
                }
                catch (Exception ex)
                {
                    _logger.Error(ex, $"CommitSetCache Error{ex.Message}");
                }
            });
            
        }

        public static void CommitSetCache<T>(this RedisClient rc, string cacheKey, T val, double expire)
        {
           
            Task.Factory.StartNew(async () =>
            {
                try
                {
                    await rc.SetAsync(cacheKey, val, expire);
                }
                catch (Exception ex)
                {
                    _logger.Error(ex, $"CommitSetCache Error{ex.Message}");
                }
            });
        }


        public static async Task SetAsync<T, TRet>(this RedisClient rc, string cacheKey, double expire, Func<T, TRet> func, T arg)
        {
            // not found
            var val = await Task.Run(() => func(arg));
            await rc.SetAsync(cacheKey, val, expire);
        }
        //public static async Task SetAsync<T>(this RedisClient rc, string cacheKey, T val, double expire)
        //{
        //    // not found
        //        await rc.SetAsync(cacheKey, val, expire);

        //}

    }

}