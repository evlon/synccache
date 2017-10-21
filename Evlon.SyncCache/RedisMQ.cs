using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using StackExchange.Redis;

namespace SyncCache
{
    public class RedisMq 
    {
        public IRedisDatabaseFactory RedisDatabaseFactory { get; set; }

        public int Db { get; set; } = 0;
  
        private Lazy<IDatabase> _database ;
        private IDatabase GetDatebase()
        {
            return _database.Value;
        }

        public RedisMq()
        {
            _database = new Lazy<IDatabase>(() => RedisDatabaseFactory.GetDatabae(Db));
        }

        public void Push<T>(string channel, T val)
        {
            GetDatebase().ListLeftPush(channel, JsonConvert.SerializeObject(val));
        }

        public bool TryPop<T>(string channel,out T val)
        {
            var redisVal = GetDatebase().ListRightPop(channel);
            if (redisVal.HasValue)
            {
                val = JsonConvert.DeserializeObject<T>(redisVal);
                return true;
            }
            else
            {
                val = default(T);
                return false;
            }
        }

        public bool TryPop<T>(string channel, int max, out T[] arr)
        {
            var database = GetDatebase();
            bool hasValue = false;
            List<T> list = new List<T>();
            for (int i = 0; i < max; i++)
            {
                var redisVal = database.ListRightPop(channel);
                if (redisVal.HasValue)
                {
                    var val = JsonConvert.DeserializeObject<T>(redisVal);
                    list.Add(val);
                    hasValue = true;
                }
                else
                {
                    break;
                }
            }

            if (hasValue)
            {
                arr = list.ToArray();
                return true;
            }
            else
            {
                arr = null;

                return false;
            }
           
        }
    }

}
