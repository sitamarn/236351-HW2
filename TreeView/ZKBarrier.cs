using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ZooKeeperNet;

namespace TreeViewLib
{
    class ZKBarrier : IWatcher
    {
        static ZooKeeperWrapper zk = null;
        static readonly Object mutex = new Object();
        String barrierPath = null;
        String name = null;
        int size;

        public ZKBarrier(ZooKeeperWrapper handler, String barrierNodePath, int size)
        {
            if (zk == null)
            {
                zk = handler;
            }
            barrierPath = barrierNodePath;
            this.size = size;
            if (!zk.Exists(barrierPath))
            {
                zk.Create(barrierPath, 
                    new byte[0], 
                    Ids.OPEN_ACL_UNSAFE, 
                    CreateMode.Persistent);
            }
        }

        public bool Enter()
        {
            name = 
                zk.Create(barrierPath,
                new byte[0],
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.EphemeralSequential);

            while (true)
            {
                lock (mutex)
                {
                    List<String> list =
                        zk.GetChildren(barrierPath, this);
                    if (list.Count() < size)
                    {
                        System.Threading.Monitor.Wait(mutex);
                    }
                    else
                    {
                        return true;
                    }
                }
            }
        }

        public Boolean Leave()
        {
            zk.Delete(name, 0);
            while(true) {
                lock (mutex)
                {
                    List<String> lst = zk.GetChildren(barrierPath, this);
                    if (lst.Count() > 0)
                    {
                        System.Threading.Monitor.Wait(mutex);
                    }
                    else
                    {
                        return true;
                    }
                }
            }
        }

        public void Process(WatchedEvent @event)
        {
            lock (mutex)
            {
                System.Threading.Monitor.Pulse(mutex);
            }
        }
    }
}
