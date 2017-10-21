using System;

namespace SyncCache
{
    public interface IDangjianCacheServiceBase
    {
        void ResetCache(string name);

        void ResetCacheAll();

        void CommitSetCache(string name, Func<string> valueFunc);
    }

   
}