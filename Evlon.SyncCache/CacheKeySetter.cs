using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace SyncCache
{
    public class CacheKeySetter
    {
        private static NLog.ILogger _logger = LogManager.GetCurrentClassLogger();

        private readonly EnumRedisDb _db;
        private readonly string _configConnectStringName;
        private ConcurrentQueue<KeyValueObject> _keyWillSetting = new ConcurrentQueue<KeyValueObject>();
        private ManualResetEvent _evtHasWillRemoveKey = new ManualResetEvent(false);
        private Task _taskRemoveKey;

        public CacheKeySetter(EnumRedisDb db, string configConnectStringName = null)
        {
            _db = db;
            _configConnectStringName = configConnectStringName;
            _taskRemoveKey = Task.Factory.StartNew(() =>
            {
                while (true)
                {
                    try
                    {
                        //如果有新数据，或者等待的时候，有新数据进来
                         if (!_keyWillSetting.IsEmpty || _evtHasWillRemoveKey.WaitOne())
                        {

                            // has data
                            Thread.Sleep(100);
                            DoSetAddKey();
                        }
                        else
                        {
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

        private void DoSetAddKey()
        {
            int sizeOnce = 1000;
            //进行删除
            foreach (var keys in GetSetAddOnecKeys(sizeOnce))
            {
                //
                Task.Factory.StartNew(() =>
                {
                    using (var rc = RedisClient.GetClient(_db, _configConnectStringName))
                    {
                        
                        rc.BatchStringSet(keys);
                    }
                });

            }
            //设置处理完数据
            if (_keyWillSetting.IsEmpty)
            {
                _evtHasWillRemoveKey.Reset();
            }
        }

        private IEnumerable<KeyValueObject[]> GetSetAddOnecKeys(int size)
        {
            while (!_keyWillSetting.IsEmpty)
            {
                List<KeyValueObject> ret = new List<KeyValueObject>();
                for (int i = 0; i < size; i++)
                {
                    KeyValueObject key;
                    if (_keyWillSetting.TryDequeue(out key))
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


        public bool CommitSetAddKey(string key,string val, double? expire)
        {
            var k = new KeyValueObject() { Key = key,Val = val, Expire = expire};
            _keyWillSetting.Enqueue(k);

            //提示有数据了
            _evtHasWillRemoveKey.Set();
            return true;
        }
    }

    
}