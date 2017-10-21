using System;
using System.Collections.Concurrent;
using System.Net;
using System.Threading.Tasks;
using NLog;
using StackExchange.Redis;

namespace SyncCache
{
    /// <summary>
    /// 修改Redis服务器数量，需要重启
    /// </summary>
    public static class RedisClusterSubscriber
    {
        private static ILogger _logger = LogManager.GetCurrentClassLogger();
        private static ConcurrentDictionary<EndPoint, IConnectionMultiplexer> redisConnectionMultiplexers = new ConcurrentDictionary<EndPoint, IConnectionMultiplexer>();

        //
        // 摘要:
        //     Subscribe to perform some operation when a change to the preferred/active node
        //     is broadcast.
        //
        // 备注:
        //     http://redis.io/commands/subscribe
        public static void ClusterSubscribe(this IConnectionMultiplexer redis, RedisChannel channel, Action<RedisChannel, RedisValue> handler,
            CommandFlags flags = CommandFlags.None)
        {
            var redisEndPoints = redis.GetEndPoints();


            //LogShenji.GetLogger().Log(EnumShenjiEntry.System,$"订阅#订阅服务器：{redisEndPoints.Length}");

            var resules = Parallel.ForEach(redisEndPoints, ep =>
            {
                try
                {
                    var conn = redisConnectionMultiplexers.GetOrAdd(ep,
                        (endPoint) => ConnectionMultiplexer.Connect(endPoint.ToString()));

                    var subscriber = conn.GetSubscriber();

                    subscriber.Subscribe(channel, handler, flags);


                }
                catch (Exception ex)
                {
                    _logger.Error(ex, $"订阅#订阅服务器失败：{ep} 原因：{ex.Message}");
                }


            });

            //LogShenji.GetLogger().Log(EnumShenjiEntry.System, $"订阅#成功订阅服务器：{redisEndPoints.Length}个");



        }
        //
        // 摘要:
        //     Subscribe to perform some operation when a change to the preferred/active node
        //     is broadcast.
        //
        // 备注:
        //     http://redis.io/commands/subscribe
        public static async Task ClusterSubscribeAsync(this IConnectionMultiplexer redis, RedisChannel channel, Action<RedisChannel, RedisValue> handler,
            CommandFlags flags = CommandFlags.None)
        {
            await Task.Factory.StartNew(() =>
            {


                var redisEndPoints = redis.GetEndPoints();


                var result = Parallel.ForEach(redisEndPoints, async ep =>
                {
                    try
                    {
                        var conn = redisConnectionMultiplexers.GetOrAdd(ep,
                            (endPoint) => ConnectionMultiplexer.Connect(endPoint.ToString()));

                        var subscriber = conn.GetSubscriber();

                        await subscriber.SubscribeAsync(channel, handler, flags);

                        _logger.Info($"订阅#成功订阅服务器：{ep}");

                    }
                    catch (Exception ex)
                    {
                        _logger.Error(ex, $"订阅#订阅服务器失败：{ep} 原因：{ex.Message}");
                    }

                });
            });




        }

    }
}