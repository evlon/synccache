using System;
using System.Configuration;
using StackExchange.Redis;

namespace SyncCache
{
    public interface IRedisDatabaseFactory
    {
        IDatabase GetDatabae();
        IDatabase GetDatabae(int db);
    }

    public class RedisDatabaseFactory : IRedisDatabaseFactory
    {
        private Lazy<IConnectionMultiplexer> _connectionMultiplexer;
        
        public string RedisConnectionStringName { get; set; }

        public RedisDatabaseFactory(string redisConnectionStringName)
        {
            this.RedisConnectionStringName = redisConnectionStringName;
            if (string.IsNullOrEmpty(RedisConnectionStringName))
            {
                throw new ConfigurationErrorsException("未配置，RedisConnectionStringName");
            }

            string connectionString =
                ConfigurationManager.ConnectionStrings[RedisConnectionStringName].ConnectionString;


            if (string.IsNullOrEmpty(connectionString))
            {
                throw new ConfigurationErrorsException("RedisConnectionStringName 的连接字符串为空");
            }
            _connectionMultiplexer =
                new Lazy<IConnectionMultiplexer>(() => ConnectionMultiplexer.Connect(connectionString));
        }

        public IConnectionMultiplexer GetConnectionMultiplexer()
        {
            return _connectionMultiplexer.Value;
        }

        public IDatabase GetDatabae()
        {
            return GetConnectionMultiplexer().GetDatabase();
        }

        public IDatabase GetDatabae(int db)
        {
            return GetConnectionMultiplexer().GetDatabase(db);
        }
    }
}