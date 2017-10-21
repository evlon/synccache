using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.Caching;
using StackExchange.Redis;
using NLog;

namespace SyncCache
{
    public class MemoryObjectCache 
    {
        private static NLog.ILogger _logger = LogManager.GetCurrentClassLogger();

        private readonly ObjectCache _cache;
        //private readonly CacheItemPolicy _policy;

        public MemoryObjectCache()
        {
            _cache = MemoryCache.Default;

        }

        public void AddOrUpdate<T>(string key, T val, TimeSpan expireTimeSpan)
        {
            if (((object) val) != null) //不能存储null 值
            {
                _cache.Add(key, val, new CacheItemPolicy()
                {
                    Priority = CacheItemPriority.Default,
                    SlidingExpiration = expireTimeSpan,
                    RemovedCallback = CacheEntryRemovedCallback
                });
            }
            else
            {
                // do log
            }
           
            
        }


        private void CacheEntryRemovedCallback(CacheEntryRemovedArguments arguments)
        {
            // arguments.CacheItem.Key 
        }

        public void AddOrUpdateBulk<T>(IList<T> dataList, Func<T, string> cacheKeyBuilder, TimeSpan expireTimeSpan)
        {
            foreach (var item in dataList)
            {
                AddOrUpdate(cacheKeyBuilder(item), item, expireTimeSpan);
            }
        }
        
        public bool TryGet<T>(string key, out T val)
        {
            var t = _cache.Get(key);
            if (t != null)
            {
                val = (T)t;
                return true;
            }
            else
            {
                val = default(T);
            }
            return false;
        }
        //public bool TryGetSetValue(string key, out RedisValue[] val)
        //{
        //    var t = _cache.Get(key);
        //    if (t != null)
        //    {
        //        val = (RedisValue[])t;
        //        return true;
        //    }
        //    else
        //    {
        //        val = null;
        //    }
        //    return false;
        //}
        public void Remove(string key)
        {
            _cache.Remove(key);
        }

        public IList<string> RemoveStartLike(string key)
        {
            List<string> removed = new List<string>();
            foreach (var cacheItem in _cache)
            {
                if (key.StartsWith(key))
                {
                    try
                    {
                        _cache.Remove(cacheItem.Key);
                        removed.Add(cacheItem.Key);
                    }
                    catch (Exception ex)
                    {
                        _logger.Error(ex,"RemoveStartLike Error");

                    }

                }

            }

            return removed;
        }
    }
}