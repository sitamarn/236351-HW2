using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ZooKeeperNet;

namespace TreeViewLib
{
    class ZKSynch : IWatcher 
    {
        private readonly Object mutex = new Object();
        private ZooKeeper zk = null;
        
        private String synchPath = null;
        private String synchReady = null;
        private String name = null;
        private String owner = null;
        private int size;

        private AutoResetEvent connected = new AutoResetEvent(false);

        public static void ZKBarrier(ZooKeeperWrapper handler, String synchNodePath, String owner, int size)
        {
            ZKSynch sZK = new ZKSynch(handler, synchNodePath, owner, size);
            sZK.Enter();
            Console.WriteLine("^^^^^^^^^^^^^^");
            sZK.Leave();
        }


        public ZKSynch(ZooKeeperWrapper handler, String synchNodePath, String owner, int size) 
        {
            zk = new ZooKeeper(handler.Address, new TimeSpan(0, 0, 0, handler.Timeout, 0), this);
            connected.WaitOne();

            synchPath = synchNodePath;
            this.size = size;
            this.owner = owner;
            //this.synchReady = synchPath + "/ready";

            if (null == zk.Exists(synchPath,false))
            {
                zk.Create(synchPath,
                    new byte[0],
                    Ids.OPEN_ACL_UNSAFE,
                    CreateMode.Persistent);
            }
        }

        public void Enter()
        {
            name =
                zk.Create(synchPath + "/b",
                new byte[0],
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.EphemeralSequential);    // Create a node for self

            while (true)
            {
                lock (mutex)
                {
                    var children = zk.GetChildren(synchPath, true);
                    Console.WriteLine("Barrier children: " + String.Join(",",children.ToArray()) + " " + children.Count());
                    if (children.Count() < size)
                    {
                        System.Threading.Monitor.Wait(mutex);
                    }
                    else
                    {
                        return;
                    }
                }
            }
        }

        public bool Leave()
        {
            zk.Delete(name, 0);

            while (true)
            {
                lock (mutex)
                {
                    var list = zk.GetChildren(synchPath, true);
                    if (list.Count() > 0)
                    {
                        System.Threading.Monitor.Pulse(mutex);
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
            if (@event.State == KeeperState.SyncConnected && @event.Type == EventType.None)
            {
                connected.Set();
            }
            if (@event.Type == EventType.NodeChildrenChanged)
            {
                lock (mutex)
                {
                    System.Threading.Monitor.Pulse(mutex);
                }
            }
        }
    }
}
