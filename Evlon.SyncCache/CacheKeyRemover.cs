using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using NLog;

namespace SyncCache
{
    public class CacheKeyRemover
    {
        private static NLog.ILogger _logger = LogManager.GetCurrentClassLogger();
        private readonly EnumRedisDb _db;
        private ConcurrentQueue<string> _keyWillRemove = new ConcurrentQueue<string>();
        private ManualResetEvent _evtHasWillRemoveKey = new ManualResetEvent(false);
        private Task _taskRemoveKey;

        public CacheKeyRemover(EnumRedisDb db)
        {
            _db = db;
            _taskRemoveKey = Task.Factory.StartNew(() =>
            {
                var stopWatcher = Stopwatch.StartNew();
                while (true)
                {
                    try
                    {
                        //如果有新数据，或者等待的时候，有新数据进来
                        if (!_keyWillRemove.IsEmpty || _evtHasWillRemoveKey.WaitOne())
                        {

                            // has data
                            Thread.Sleep(100);
                            DoRemoveKey();

                            
                        }
                        else
                        {
                            if (stopWatcher.Elapsed.Minutes > 30)
                            {
                                stopWatcher.Restart();

                                _logger.Info("CacheKeyRemover Datemon Thread Alive!");
                            }
                            //no data
                            continue;
                        }
                    }
                    catch (ThreadAbortException)
                    {
                        //NlogComm.NLogHel.("clear cache thread abort");
                        return;
                    }
                    catch (Exception ex)
                    {
                        //log
                        _logger.Error( ex, "clear cache error");
                    }

                }

            }, TaskCreationOptions.LongRunning);
           

        }

        private void DoRemoveKey()
        {
            int sizeOnce = 1000;
            //进行删除
            foreach (var keys in GetRemoveOnecKeys(sizeOnce))
            {
                using (var rc = RedisClient.GetClient(_db))
                {
                    rc.BatchKeyRemove(keys);
                    _logger.Debug($"CacheKeyRemover 批量删除{keys.Length}个键值。");
                }


                //
                //Task.Factory.StartNew(async () =>
                //{
                //    using (var rc = RedisClient.GetClient(_db))
                //    {
                //        await rc.RemoveAsync(keys);
                //    }
                //});

            }
            //设置处理完数据
            if (_keyWillRemove.IsEmpty)
            {
                _evtHasWillRemoveKey.Reset();
            }
        }

        private IEnumerable<string[]> GetRemoveOnecKeys(int size)
        {
            while (!_keyWillRemove.IsEmpty)
            {
                List<string> ret = new List<string>();
                for (int i = 0; i < size; i++)
                {
                    string key;
                    if (_keyWillRemove.TryDequeue(out key))
                    {
                        ret.Add(key);
                    }
                    else
                    {
                        break;
                    }

                }
                yield return ret.ToArray();
            }
        }
       

        public bool CommitRemoveKey(string key)
        {
            _keyWillRemove.Enqueue(key);

            //提示有数据了
            _evtHasWillRemoveKey.Set();
            return true;
        }
    }
}