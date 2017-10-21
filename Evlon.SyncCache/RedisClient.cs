using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Web;
using StackExchange.Redis;
using System.Threading.Tasks;
using System.Collections.Specialized;
using Newtonsoft.Json;
using NLog;
using StackExchange.Redis.KeyspaceIsolation;

namespace SyncCache
{

    public class RedisConnectionMultiplexer
    {
        private static ConcurrentDictionary<string, ConnectionMultiplexer> _connectionMultiplexers = new ConcurrentDictionary<string, ConnectionMultiplexer>();
        public static ConnectionMultiplexer GetConnectionMultiplexer(string connectionString)
        {
            return _connectionMultiplexers.GetOrAdd(connectionString, connstr =>
            {
                var ret = ConnectionMultiplexer.Connect(connstr);

                return ret;
            });
        }
    }

    public class RedisClient : IDisposable
    {
        private static ILogger _logger = LogManager.GetCurrentClassLogger();
        private readonly Lazy<ConnectionMultiplexer> redis;

        private readonly Lazy<IDatabase> _redisDatabase;
        private readonly int _db;

        public static string DefaultConnectionString
        {
            get
            {
                return  ConfigurationManager.ConnectionStrings["redisConnection"].ConnectionString;
            }
        }



       

        private RedisClient(EnumRedisDb db,string configConnectStringName = null)
        {
            _db = 0;//(int)db;
            var configConnect = string.IsNullOrEmpty(configConnectStringName) ? DefaultConnectionString : 
                ConfigurationManager.ConnectionStrings[configConnectStringName].ConnectionString;
            //if(redis == null) { 
            //redis = new Lazy<ConnectionMultiplexer>(() =>
            //{
            //    //var hostIps = configConnect.Split(";".ToCharArray(), StringSplitOptions.RemoveEmptyEntries);
            //    //StackExchange.Redis.ConfigurationOptions options = new ConfigurationOptions();
            //    //options.AbortOnConnectFail = false;
            //    //Array.ForEach(hostIps, d =>
            //    //{
            //    //    var ipport = d.Split(':');
            //    //    var ip = IPAddress.Parse(ipport[0]);
            //    //    var port = int.Parse(ipport[1]);

            //    //    var endPoint = new IPEndPoint(ip, port);
            //    //    options.EndPoints.Add(endPoint);
            //    //});
            //    var ret = ConnectionMultiplexer.Connect(configConnect);

            //    return ret;
            //});

            redis = new Lazy<ConnectionMultiplexer>(()=>RedisConnectionMultiplexer.GetConnectionMultiplexer(configConnect));

            _redisDatabase = new Lazy<IDatabase>(()=> GetRedis().GetDatabase());
        }

        public ConnectionMultiplexer GetRedis()
        {
            return redis.Value;
        }

        public IDatabase GetDatabase()
        {
            return _redisDatabase.Value;
        }

        //public ISubscriber GetRedisSubscriber(object asyncState)
        //{
        //    return redis.GetSubscriber(asyncState);
        //}


        public void BatchKeyRemove(string[] keys)
        {
            var batch = GetDatabase().CreateBatch();
            Array.ForEach(keys, d =>
            {
                batch.KeyDeleteAsync(d);
            });

            batch.Execute();


            Console.WriteLine($"BatchKeyRemove count:{keys.Length}");

        }

        public void BatchStringSet(KeyValueObject[] keyValueObjects)
        {
            var batch = GetDatabase().CreateBatch();
            Array.ForEach(keyValueObjects, d =>
            {
                batch.StringSetAsync(d.Key, d.Val);
                if(d.Expire.HasValue)
                    batch.KeyExpireAsync(d.Key, TimeSpan.FromSeconds(d.Expire.Value));
            });

            batch.Execute();


            Console.WriteLine($"BatchStringSet count:{keyValueObjects.Length}");

        }
        public void BatchSetAdd(string key, string[] values)
        {
            var batch = GetDatabase().CreateBatch();
            foreach (var value in values)
            {
                batch.SetAddAsync(key, value);
            }
            batch.Execute();

            Console.WriteLine($"BatchSetAdd count:{values.Length}");

        }

        public static RedisClient GetClient(EnumRedisDb db, string configConnectStringName = null)
        {
            return new RedisClient(db, configConnectStringName);
        }

        public static RedisClient GetClient(string configConnectStringName = null, EnumRedisDb db = EnumRedisDb.Default)
        {
            return new RedisClient(db, configConnectStringName);
        }


        #region 辅助方法

        public long GetFirstServerDbSize()
        {
            //return redis.Value.GetServer(redisServer).DatabaseSize((int)_db);
            ////TO DO:此处按照单台服务器获取dbsize, 不支持集群，需要部署单台Redis
            var endPoints = redis.Value.GetEndPoints();
            if (endPoints.Length > 0)
            {
                 return redis.Value.GetServer(endPoints.First()).DatabaseSize();
            }
            return 0;
            //return endPoints.Sum(endPoint =>
            //{
            //    return redis.Value.GetServer(redisServer).DatabaseSize((int)_db);

            //});
            //return _options.EndPoints.Cast<IPEndPoint>().Sum(ipEndPoint => _redisConn.GetServer(ipEndPoint.Address.ToString(), ipEndPoint.Port).DatabaseSize((int)Db));
        }

        public bool Set<T>(string key, T obj)
        {
            return GetDatabase().StringSet(key, JsonConvert.SerializeObject(obj));
        }

        public bool Set<T>(string key, T obj, double expiry)
        {
            return GetDatabase().StringSet(key, JsonConvert.SerializeObject(obj), TimeSpan.FromSeconds(expiry));
        }

        public bool Set<T>(string key, T obj, TimeSpan expiry)
        {
            return GetDatabase().StringSet(key, JsonConvert.SerializeObject(obj), expiry);
        }

        public async Task<bool> SetAsync<T>(string key, T obj, double expiry)
        {
            return await GetDatabase().StringSetAsync(key, JsonConvert.SerializeObject(obj), TimeSpan.FromSeconds(expiry));
        }

        public async Task<bool> SetAsync<T>(string key,T obj,TimeSpan? expire = null)
        {
            return await GetDatabase().StringSetAsync(key, JsonConvert.SerializeObject(obj), expire);
        }
        public bool TryGet<T>(string key, out T val)
        {
            var redisValue = GetDatabase().StringGet(key);
            if (redisValue.HasValue)
            {
                val = JsonConvert.DeserializeObject<T>(redisValue);
                return true;
            }
            else
            {
                val = default(T);
                return false;
            }
        }
       
        public T Get<T>(string key)
        {
            var redisValue = GetDatabase().StringGet(key);
            if (redisValue.HasValue)
            {
                return JsonConvert.DeserializeObject<T>(redisValue);
            }
            return default(T);
        }
        public RedisValue[] GetSetValue(string key)
        {
             return GetDatabase().SetMembers(key); 
        }

        public bool SetAddValue(string key, string val)
        {
            return GetDatabase().SetAdd(key, val);
        }
        public async Task<bool> SetAddValueAsync(string key, string val, TimeSpan? expire = null)
        {
            var db = GetDatabase();
            var ret = await db.SetAddAsync(key, val);
            if (expire.HasValue)
                await db.KeyExpireAsync(key, expire);

            return ret;
        }
        public long SetAddValue(string key, string[] val,TimeSpan? expire = null)
        {
            var db = GetDatabase();
            var ret = db.SetAdd(key, Array.ConvertAll(val,v=>(RedisValue)v));
            if(expire.HasValue)
                db.KeyExpire(key, expire);

            return ret;
        }
        public async Task<long> SetAddValueAsync(string key, string[] val)
        {
            return await GetDatabase().SetAddAsync(key, Array.ConvertAll(val, v => (RedisValue)v));
        }

        public bool SetRemoveValue(string key, string val)
        {
            return GetDatabase().SetRemove(key, val);
        }
        public long SetRemoveValue(string key, string[] val)
        {
            return GetDatabase().SetRemove(key, Array.ConvertAll(val, v => (RedisValue)v));
        }

        public async Task<bool> SetRemoveValueAsync(string key, string val)
        {
            return await GetDatabase().SetRemoveAsync(key, val);
        }
        public async Task<long> SetRemoveValueAsync(string key, string[] val)
        {
            return await GetDatabase().SetRemoveAsync(key, Array.ConvertAll(val, v => (RedisValue)v));
        }

        public async Task<T> GetAsync<T>(string key)
        {
            var redisValue = await GetDatabase().StringGetAsync(key);
            if (redisValue.HasValue)
            {
                return JsonConvert.DeserializeObject<T>(redisValue);
            }
            return default(T);
        }

        public bool CommitRemoveCacheAction(string key)
        {
            return RedisCache.CommitRemoveKey((EnumRedisDb)_db, key);
           // Task.Factory.StartNew(() => GetDatabase().KeyDeleteAsync(key));
           // return GetDatabase().KeyDelete(key);
            //return true;
        }

        public bool KeyDelete(string key)
        {
            return GetDatabase().KeyDelete(key);
        }

        public async Task<bool> KeyDeleteAsync(string key)
        {
            return await GetDatabase().KeyDeleteAsync(key);
        }





        //public async Task<bool> RemoveAsync(string[] keys)
        //{
        //    var redisKeys = Array.ConvertAll(keys,k => 
        //            new {Slot = GetDatabase().Multiplexer.HashSlot(k), Key=(RedisKey)k})
        //        .GroupBy(d=>d.Slot).ToDictionary(g=>g.Key,g=>g.Select(d=>d.Key).ToArray());

        //    long totalRemoved = 0;
        //    foreach (var redisKey in redisKeys)
        //    {
        //        if (redisKey.Value.Length > 1)
        //        {
        //            try
        //            {
        //                //var batch =  GetDatabase().CreateBatch();
        //                //batch.

        //                var removed = await GetDatabase().KeyDeleteAsync(redisKey.Value);
        //                Console.WriteLine($"RemoveAsync count:{removed}");

        //                totalRemoved += removed;

        //            }
        //            catch (Exception ex)
        //            {
        //                _logger.Error(ex, "RemoveAsync Error");
        //                throw;
        //            }
        //        }
        //        else
        //        {
        //            try
        //            {
        //                //var batch =  GetDatabase().CreateBatch();
        //                //batch.

        //                var isRemoved = await GetDatabase().KeyDeleteAsync(redisKey.Value[0]);
        //                Console.WriteLine($"RemoveAsync count:1");

        //                totalRemoved += (isRemoved ? 1: 0);

        //            }
        //            catch (Exception ex)
        //            {
        //                _logger.Error(ex, "RemoveAsync Error");
        //                throw;
        //            }
        //        }
        //    }

        //    return totalRemoved > 0;

        //}

        public bool KeyExists(string key)
        {
            return GetDatabase().KeyExists(key);
        }

        public async Task<bool> KeyExistsAsync(string key)
        {
            return await GetDatabase().KeyExistsAsync(key);
        }

        public bool HashSet<TValue>(string key, string hashField, TValue obj)
        {
            var result = false;
            if (obj != null)
            {
                result = GetDatabase().HashSet(key, hashField, JsonConvert.SerializeObject(obj));
            }
            return result;
        }

        public async Task<bool> HashSetAsync<TValue>(string key, string hashField, TValue obj)
        {
            var result = false;
            if (obj != null)
            {
                result = await GetDatabase().HashSetAsync(key, hashField, JsonConvert.SerializeObject(obj));
            }
            return result;
        }

        public void HashSet<TValue>(string key, Dictionary<string, TValue> dic)
        {
            if (dic == null) return;
            var hashEntrys = dic.Select(kv => new HashEntry(kv.Key, JsonConvert.SerializeObject(kv.Value))).ToArray();
            GetDatabase().HashSet(key, hashEntrys);
        }


        public async Task HashSetAsync<TValue>(string key, Dictionary<string, TValue> dic)
        {
            if (dic != null)
            {
                var hashEntrys = dic.Select(kv => new HashEntry(kv.Key, JsonConvert.SerializeObject(kv.Value))).ToArray();
                await GetDatabase().HashSetAsync(key, hashEntrys);
            }
        }

        public bool HashDelete(string key, string hashField)
        {
            return GetDatabase().HashDelete(key, hashField);
        }

        public async Task<bool> HashDeleteAsync(string key, string hashField)
        {
            return await GetDatabase().HashDeleteAsync(key, hashField);
        }

        public TValue HashGet<TValue>(string key, string hashKey)
        {
            var strValue = GetDatabase().HashGet(key, hashKey);
            return strValue.HasValue ? JsonConvert.DeserializeObject<TValue>(strValue) : default(TValue);
        }

        public async Task<TValue> HashGetAsync<TValue>(string key, string hashKey)
        {
            var strValue = await GetDatabase().HashGetAsync(key, hashKey);
            return strValue.HasValue ? JsonConvert.DeserializeObject<TValue>(strValue) : default(TValue);
        }

        public Dictionary<string, TValue> HashGetAll<TValue>(string key)
        {
            var hashValues = GetDatabase().HashGetAll(key);
            var dic = new Dictionary<string, TValue>();
            if (hashValues == null) return dic;
            foreach (var hashValue in hashValues)
            {
                dic.Add(hashValue.Name, JsonConvert.DeserializeObject<TValue>(hashValue.Value));
            }
            return dic;
        }

        public async Task<Dictionary<string, TValue>> HashGetAllAsync<TValue>(string key)
        {
            var hashValues = await GetDatabase().HashGetAllAsync(key);
            var dic = new Dictionary<string, TValue>();
            if (hashValues == null) return dic;
            foreach (var hashValue in hashValues)
            {
                dic.Add(hashValue.Name, JsonConvert.DeserializeObject<TValue>(hashValue.Value));
            }
            return dic;
        }

        //public T Get<T>(string key, Func<T> func, Func<T, bool> isValueCanCache = null)
        //{
        //    T result;
        //    var value = GetDatabase().StringGet(key);
        //    if (value.HasValue)
        //    {
        //        result = JsonConvert.DeserializeObject<T>(value);
        //    }
        //    else
        //    {
        //        result = func();
        //        var task = Task.Factory.StartNew(async () =>
        //        {
        //            await SetAsync(key, result);
        //        });
        //       // Task.WaitAll(task); //NKL,为什么等？
        //    }
        //    return result;
        //}

        public T Get<T>(string key, Func<T> func, TimeSpan? expiry = null, Func<T,bool> isValueCanCache = null)
        {
            T result;
            var value = GetDatabase().StringGet(key);
            if (!value.IsNull && value.HasValue)
            {
                result = JsonConvert.DeserializeObject<T>(value);
            }
            else
            {
                result = func();
                if (isValueCanCache == null ||  isValueCanCache(result))
                {
                    Task.Factory.StartNew(async () =>
                    {
                        await SetAsync(key, result, expiry);
                    });
                }
                //Task.WaitAll(task);NKL,为什么等？
            }
            return result;
        }

        public async Task<T> GetAsync<T>(string key, Func<Task<T>> funcAsync)
        {
            T result;
            var value = await GetDatabase().StringGetAsync(key);
            if (value.HasValue)
            {
                result = JsonConvert.DeserializeObject<T>(value);
            }
            else
            {
                result = await funcAsync();
                await Task.Factory.StartNew(async () =>
                 {
                     await SetAsync(key, result);
                 });
            }
            return result;
        }

        public async Task<T> GetAsync<T>(string key, Func<T> func, TimeSpan expire, Func<T, bool> isValueCanCache = null)
        {
            T result;
            var value = await GetDatabase().StringGetAsync(key);
            if (value.HasValue)
            {
                result = JsonConvert.DeserializeObject<T>(value);
            }
            else
            {
                result = func();
                if (isValueCanCache == null || isValueCanCache(result))
                {
                    await SetAsync(key, result, expire.TotalSeconds);
                }
            }
            return result;
        }

        #endregion

        public void Dispose()
        {
        }

        public static RedisClient GetClientTongji()
        {
            return GetClient(EnumRedisDb.Tongji);
        }


        public static RedisClient GetClientZuzhi()
        {
            return GetClient(EnumRedisDb.Zuzhi);
        }

        public static RedisClient GetClientOnline()
        {
            return GetClient(EnumRedisDb.Online);
        }


        public static RedisClient GetClientDangyan()
        {
            return GetClient(EnumRedisDb.Dangyuan);
        }

        public static RedisClient GetClientApp()
        {
            return GetClient(EnumRedisDb.App);
        }

        public static RedisClient GetClientOther()
        {
            return GetClient(EnumRedisDb.Default);
        }
    }
}