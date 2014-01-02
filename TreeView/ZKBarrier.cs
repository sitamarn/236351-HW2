using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ZooKeeperNet;

namespace TreeViewLib
{
    public class ZKBarrier : IWatcher
    {
        private AutoResetEvent connected = new AutoResetEvent(false);
        private ZKSynch first = null;
        private ZKSynch second = null;


        public static ZKBarrier Barrier(ZooKeeperWrapper handler, String synchNodePath, String owner, int size)
        {
            ZKBarrier sZK = new ZKBarrier(handler, synchNodePath, owner, size);
            sZK.Enter();
            return sZK;
        }

        public ZKBarrier(ZooKeeperWrapper handler, String barrierPath, String owner, int size) 
        {
            ZooKeeper zk = new ZooKeeper(handler.Address, new TimeSpan(0, 0, 0, handler.Timeout, 0), this);
            connected.WaitOne();

            if (null == zk.Exists(barrierPath,false))
            {
                zk.Create(barrierPath,
                    new byte[0],
                    Ids.OPEN_ACL_UNSAFE,
                    CreateMode.Persistent);
            }

            first = new ZKSynch(handler, barrierPath + "/first_barrier", owner, size);
            second= new ZKSynch(handler, barrierPath + "/second_barrier", owner, size);
        }

        public void Process(WatchedEvent @event)
        {
            if (@event.State == KeeperState.SyncConnected && @event.Type == EventType.None)
            {
                connected.Set();
            }
        }

        public void Enter()
        {
            first.Enter();
            second.Enter();
        }

        public void Leave()
        {
            first.Leave();
            second.Leave();
        }
    }
}
