using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace UseSample
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("你运行程序后，可以直接在Redis中修改memcache#msg的值，然后在这里看到立即更新后的值。");
            /*

            127.0.0.1:63791> keys *
            1) "memcache#sample#msg"
            127.0.0.1:63791> get "memcache#sample#msg"
            "\"hello world\""
            127.0.0.1:63791> set "memcache#sample#msg" "\"nihao\""
            OK
            */
            var syncCache = new SyncCache.Mem4RedisCache("sample", 24000);
            while (true)
            {
                var msg = syncCache.GetValue<string>("sample#msg",()=>"hello world");
                Console.WriteLine(msg);
                Thread.Sleep(200);
            }
        }
    }
}
